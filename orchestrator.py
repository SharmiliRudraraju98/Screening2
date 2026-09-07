"""Level-by-level deployment of the approved assets.

Flow:
  1. One full paged read of GET /s2/destination -> snapshot dict.
  2. Build the dependency graph, folding in prior status from the
     destination + journal (deployed / already_present / duplicated /
     unconfirmed).
  3. For each level, lowest first:
       a. Concurrently (max 25 workers) run snapshot-aware deploy_asset
          on every pending asset in the level, each with its bound
          idempotency key.
       b. Collect assets whose result is UNCONFIRMED-from-ambiguity into
          a per-level list.
       c. Resolve that list SERIALLY: one redeploy() each, which does its
          own single fresh GET /s2/destination. (redeploy reuses the bound
          key, so it is safe: worst case the provider replays.)
       d. Only advance once every asset in the level is terminal.
       e. Log a level summary.
  4. Return the full {asset_id: result} map for the report builder.

The snapshot is read ONCE (step 1). Within a level, deploy_asset makes at
most one destination GET per asset and only on a 5xx/timeout, so a clean
level of N assets is ~N POSTs + a trickle of GETs -- well under the rate
limit that a GET-per-asset pre-check tripped.

Between levels the snapshot is NOT refreshed from the network by default:
what we deployed in level k is folded into the in-memory snapshot so
level k+1's children see their parents as present. (A stale snapshot only
ever causes an extra harmless deploy attempt that the idempotency key
collapses; it never causes a duplicate.)
"""

import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

from client import HttpClient
from deploy import (BLOCKED, DEPLOYED, ALREADY_PRESENT, DUPLICATED, UNCONFIRMED,
                    build_snapshot, deploy_asset, redeploy, NeedsHumanDecision)
from graph import DependencyGraph
from journal import Journal
from reconcile import prior_status

TERMINAL = {DEPLOYED, ALREADY_PRESENT, BLOCKED, UNCONFIRMED, DUPLICATED}
MAX_WORKERS = 25


class Orchestrator:
    def __init__(self, assets, *, client=None, journal=None, max_workers=MAX_WORKERS):
        self.assets = {a["id"]: a for a in assets}
        self.client = client or HttpClient()
        self.journal = journal or Journal()
        self.max_workers = max_workers
        self.results = {}          # asset_id -> result dict
        self.snapshot = {}         # asset_id -> [rows], read once then updated

    # -- setup --------------------------------------------------------------

    def prepare(self):
        """Read the snapshot once, build the graph with prior status."""
        self.snapshot = build_snapshot(self.client)
        prior, counts = prior_status(client=self.client, journal=self.journal)
        self.graph = DependencyGraph(list(self.assets.values()),
                                     prior_status=prior)

        # Seed results with everything already decided by the graph.
        for aid, reason in self.graph.blocked.items():
            self.results[aid] = {"asset_id": aid, "status": BLOCKED,
                                 "reason": reason, "dep_rows": []}
        for aid, status in self.graph.decided.items():
            self.results[aid] = {"asset_id": aid, "status": status,
                                 "reason": "decided from destination snapshot",
                                 "dep_rows": self.snapshot.get(aid, [])}
        # prior BAD (duplicated / unconfirmed) that are not graph-blocked
        for aid, status in prior.items():
            if aid in self.results:
                continue
            if status in (DUPLICATED, UNCONFIRMED):
                self.results[aid] = {"asset_id": aid, "status": status,
                                     "reason": f"prior status: {status}",
                                     "dep_rows": self.snapshot.get(aid, [])}
        return self

    # -- run --------------------------------------------------------------

    def run(self, *, levels=None, log=print):
        """Deploy level by level. `levels` optionally restricts which."""
        by_level = self.graph.pending_by_level()
        wanted = sorted(by_level) if levels is None else sorted(
            l for l in by_level if l in levels)

        for level in wanted:
            pending = [a for a in by_level[level]
                       if self.results.get(a, {}).get("status") not in TERMINAL]
            if not pending:
                log(f"level {level}: nothing pending, skipping")
                continue
            self._run_level(level, pending, log=log)

        return self.results

    def _run_level(self, level, pending, *, log):
        log(f"\n=== level {level}: {len(pending)} pending "
            f"(concurrency {self.max_workers}) ===")
        t0 = time.monotonic()

        # a. concurrent pass
        ambiguous = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futs = {ex.submit(self._deploy_one, aid): aid for aid in pending}
            for fut in as_completed(futs):
                aid = futs[fut]
                res = fut.result()
                self.results[aid] = res
                if res["status"] == UNCONFIRMED and res.get("_ambiguous"):
                    ambiguous.append(aid)

        # b/c. resolve ambiguous serially
        if ambiguous:
            log(f"  {len(ambiguous)} ambiguous -> resolving serially: {ambiguous}")
            for aid in ambiguous:
                self.results[aid] = self._resolve_one(aid, log=log)

        # d. fold newly-deployed rows into the in-memory snapshot so the
        #    next level's children see their parents.
        for aid, res in self.results.items():
            if res["status"] in (DEPLOYED, ALREADY_PRESENT):
                self.snapshot.setdefault(aid, res.get("dep_rows") or [{"asset_id": aid}])

        # e. summary
        tally = Counter(self.results[a]["status"] for a in pending)
        dt = round(time.monotonic() - t0, 1)
        log(f"  level {level} done in {dt}s: {dict(tally)}")
        non_terminal = [a for a in pending
                        if self.results[a]["status"] not in TERMINAL]
        if non_terminal:
            log(f"  !!! level {level} has non-terminal assets: {non_terminal}")

    # -- per-asset --------------------------------------------------------

    def _deploy_one(self, asset_id):
        checksum = self.assets[asset_id]["checksum"]
        try:
            res = deploy_asset(asset_id, checksum, client=self.client,
                               journal=self.journal, snapshot=self.snapshot)
        except NeedsHumanDecision as exc:
            return {"asset_id": asset_id, "status": UNCONFIRMED,
                    "reason": f"needs human: {exc}", "dep_rows": [],
                    "_needs_human": True}
        # Tag results that came from the ambiguous path so the level runner
        # knows to re-resolve them serially.
        if res["status"] == UNCONFIRMED and "ambiguous" in res["reason"]:
            res["_ambiguous"] = True
        return res

    def _resolve_one(self, asset_id, *, log):
        checksum = self.assets[asset_id]["checksum"]
        try:
            res = redeploy(asset_id, checksum, client=self.client,
                           journal=self.journal)
        except NeedsHumanDecision as exc:
            log(f"    {asset_id}: NEEDS HUMAN -- {exc}")
            return {"asset_id": asset_id, "status": UNCONFIRMED,
                    "reason": f"needs human: {exc}", "dep_rows": [],
                    "_needs_human": True}
        log(f"    {asset_id}: {res['status']} ({res['reason']})")
        return res
