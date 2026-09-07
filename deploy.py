"""Deploy one asset, exactly once, and decide its status from evidence.

The rules here are deliberate and were paid for (see FINDINGS.md, the
as-0005 mess). In order:

  1. Bind a stable idempotency key for the asset and journal it BEFORE
     any network call. Key = idempotency_key_for(asset_id), reproducible.

  2. GET /s2/destination. If a row for this asset_id already exists ->
     ALREADY_PRESENT. Do not deploy.

  3. POST /s2/deploy once, with header x-cq-idempotency-key = the bound key.
       - 201            -> DEPLOYED
       - 200 replayed:true -> ALREADY_PRESENT (provider confirms no new row)
       - 409 duplicate_write_refused -> ALREADY_PRESENT (write already landed;
         but if the destination shows 0 rows, UNCONFIRMED -- do not guess)
       - 409 parent_not_deployed -> BLOCKED (write refused, nothing created;
         the parent named in the body is not in the destination yet)
       - 500 / timeout / anything else ambiguous -> do NOT resend blindly.
         GET /s2/destination once (visibility is instant) and decide:
           exactly one row for this asset_id -> DEPLOYED
           no row                            -> UNCONFIRMED (leave it alone)
           more than one row                 -> DUPLICATED (report honestly)

  4. A resend is only ever allowed with the SAME bound key. If anything
     would require a different key, this module raises NeedsHumanDecision
     rather than guess -- that is exactly the mistake that made as-0005.

This module contains NO retry loop. It sends at most one deploy POST and
at most two destination GETs per call. Retrying a whole asset is a
higher-layer decision, and even then only via redeploy() with the same key.
"""

from client import HttpClient
from journal import Journal, idempotency_key_for

DEPLOY_PATH = "/s2/deploy"
DESTINATION_PATH = "/s2/destination"
IDEMPOTENCY_HEADER = "x-cq-idempotency-key"

# status values
DEPLOYED = "deployed"
ALREADY_PRESENT = "already_present"
UNCONFIRMED = "unconfirmed"
DUPLICATED = "duplicated"
BLOCKED = "blocked"

# The provider returns 409 for two unrelated reasons. Only one means the
# write landed.
ERR_DUPLICATE = "duplicate_write_refused"   # write already happened -> present
ERR_PARENT = "parent_not_deployed"          # write refused, nothing created


class NeedsHumanDecision(RuntimeError):
    """Raised when proceeding safely is not possible without a human call."""


def read_all_destination_rows(client=None):
    """Every deploy row in the destination, deduplicated by its own row id.

    GET /s2/destination paginates with an overlapping window -- the same
    dep-N row comes back on two consecutive pages, byte-identical (the same
    bug GET /s2/assets has). Counting rows per asset_id without collapsing
    by row id invents phantom duplicates. Everything that needs to know
    "how many real rows does this asset have" must go through here.
    """
    client = client or HttpClient()
    by_row_id = {}
    cursor = None
    while True:
        params = {"cursor": cursor} if cursor is not None else None
        resp = client.get(DESTINATION_PATH, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"GET {DESTINATION_PATH} -> {resp.status_code} {resp.reason}: "
                f"{resp.text[:200]}")
        data = resp.json()
        for row in data.get("items", []):
            existing = by_row_id.get(row["id"])
            if existing is not None and existing != row:
                raise RuntimeError(
                    f"destination row {row['id']} returned twice with "
                    f"different content: {existing!r} vs {row!r}")
            by_row_id[row["id"]] = row
        cursor = data.get("next_cursor")
        if not cursor:
            break
    return list(by_row_id.values())


def build_snapshot(client=None):
    """One full paged read of GET /s2/destination -> {asset_id: [rows]}.

    Rows are deduplicated by row id (see read_all_destination_rows). Take
    this ONCE before a batch; it is stale the moment we start writing.
    """
    client = client or HttpClient()
    snap = {}
    for row in read_all_destination_rows(client):
        snap.setdefault(row["asset_id"], []).append(row)
    return snap


def _destination_rows(client, asset_id):
    """The real destination rows for one asset, deduplicated by row id."""
    return [r for r in read_all_destination_rows(client)
            if r.get("asset_id") == asset_id]


def deploy_asset(asset_id, checksum, *, client=None, journal=None,
                 snapshot=None):
    """Deploy one asset, exactly once, deciding its status from evidence.

    Returns {"asset_id", "status", "reason", "dep_rows": [...]}. status is
    one of DEPLOYED / ALREADY_PRESENT / BLOCKED / UNCONFIRMED / DUPLICATED.
    Raises NeedsHumanDecision if a safe path forward requires a human.

    snapshot: a pre-fetched {asset_id: [rows]} dict from ONE full read of
      GET /s2/destination taken before this batch started. When given, the
      up-front "already present?" check reads this dict instead of making
      its own GET -- that is what lets a whole level deploy at concurrency
      25 without a burst of destination reads. If asset_id is absent from
      the snapshot, we proceed to deploy normally.

      The snapshot is NOT consulted after the deploy POST: it is stale for
      anything we just wrote. A 5xx / timeout still triggers exactly one
      fresh GET /s2/destination for this specific asset (the ambiguous
      path), unchanged.

    snapshot=None keeps the old behaviour: a live pre-check GET per asset.
    That path is retained as a fallback; the orchestrator does not use it.
    """
    client = client or HttpClient()
    journal = journal or Journal()

    # 1. Bind + journal the key BEFORE any request.
    key = idempotency_key_for(asset_id)
    journal.bind_key(asset_id, key)

    # 2. "Already present?" -- from the snapshot if we have one, else a
    #    live GET (fallback path).
    if snapshot is not None:
        rows = list(snapshot.get(asset_id, []))
        source = "snapshot"
    else:
        rows = _destination_rows(client, asset_id)
        source = "live pre-check"

    if len(rows) > 1:
        journal.record("outcome", asset_id, status=DUPLICATED,
                       reason=f"{source} found {len(rows)} rows", dep_rows=rows)
        return _result(asset_id, DUPLICATED,
                       f"{len(rows)} rows already in destination ({source})", rows)
    if len(rows) == 1:
        journal.record("outcome", asset_id, status=ALREADY_PRESENT,
                       reason=f"{source} found existing row", dep_rows=rows)
        return _result(asset_id, ALREADY_PRESENT,
                       f"row already in destination before deploy ({source})", rows)

    # 3. Deploy once, with the bound key.
    journal.record("deploy_attempt", asset_id, idempotency_key=key,
                   checksum=checksum)
    try:
        resp = client.post(DEPLOY_PATH,
                           json_body={"asset_id": asset_id, "checksum": checksum},
                           headers={IDEMPOTENCY_HEADER: key})
    except Exception as exc:  # transport failure / timeout
        return _ambiguous(asset_id, client, journal, key,
                          f"transport error: {type(exc).__name__}: {exc}")

    return _decide_from_response(asset_id, resp, client, journal, key)


def _decide_from_response(asset_id, resp, client, journal, key):
    """Classify a deploy POST response. No destination read on the clean
    paths (201 / 200-replayed / 409); one read only on ambiguity."""
    status_code = resp.status_code
    body = _safe_json(resp)

    if status_code == 201:
        # Trust the 201. The snapshot is stale and re-reading here is the
        # per-asset GET we are trying to avoid. If a concurrent writer also
        # created a row, the final reconcile catches it.
        journal.record("outcome", asset_id, status=DEPLOYED,
                       reason="201 created", http=201)
        return _result(asset_id, DEPLOYED, "201 created", [])

    if status_code == 200 and isinstance(body, dict) and body.get("replayed"):
        journal.record("outcome", asset_id, status=ALREADY_PRESENT,
                       reason="200 replayed:true", http=200)
        return _result(asset_id, ALREADY_PRESENT,
                       "200 replayed:true -- provider made no new row", [])

    if status_code == 409:
        err = body.get("error") if isinstance(body, dict) else None

        if err == ERR_PARENT:
            # Write REFUSED, nothing created. Body names the missing parent.
            missing = body.get("depends_on") if isinstance(body, dict) else None
            journal.record("outcome", asset_id, status=BLOCKED,
                           reason=f"409 parent_not_deployed ({missing})",
                           http=409, body=body)
            return _result(asset_id, BLOCKED,
                           f"parent {missing} not deployed", [])

        if err == ERR_DUPLICATE:
            # Write already landed on an earlier attempt. Confirm with one
            # read -- this is the one case where a 409 needs the truth.
            rows = _destination_rows(client, asset_id)
            if len(rows) > 1:
                journal.record("outcome", asset_id, status=DUPLICATED,
                               reason="409 duplicate but >1 row", dep_rows=rows)
                return _result(asset_id, DUPLICATED,
                               f"409 duplicate and {len(rows)} rows present", rows)
            if len(rows) == 0:
                journal.record("outcome", asset_id, status=UNCONFIRMED,
                               reason="409 duplicate but 0 rows in destination",
                               dep_rows=rows)
                return _result(asset_id, UNCONFIRMED,
                               "409 duplicate_write_refused but no row present",
                               rows)
            journal.record("outcome", asset_id, status=ALREADY_PRESENT,
                           reason="409 duplicate_write_refused", http=409,
                           body=body, dep_rows=rows)
            return _result(asset_id, ALREADY_PRESENT,
                           "409 duplicate_write_refused -- already deployed", rows)

        # Some other 409 we have not seen. Ambiguous, do not guess.
        return _ambiguous(asset_id, client, journal, key,
                          f"HTTP 409 unknown error: {body!r}")

    # 500 / 504 / unexpected 4xx / anything else: ambiguous. Read once.
    return _ambiguous(asset_id, client, journal, key,
                      f"HTTP {status_code}: {body!r}")


def _ambiguous(asset_id, client, journal, key, detail):
    """One destination read, then decide. Never resends."""
    journal.record("ambiguous", asset_id, detail=detail, bound_key=key)
    rows = _destination_rows(client, asset_id)

    if len(rows) == 1:
        journal.record("outcome", asset_id, status=DEPLOYED,
                       reason=f"ambiguous ({detail}); destination shows 1 row",
                       dep_rows=rows)
        return _result(asset_id, DEPLOYED,
                       f"ambiguous response, but destination shows exactly "
                       f"one row ({detail})", rows)

    if len(rows) == 0:
        journal.record("outcome", asset_id, status=UNCONFIRMED,
                       reason=f"ambiguous ({detail}); destination shows no row",
                       dep_rows=rows)
        return _result(asset_id, UNCONFIRMED,
                       f"ambiguous response and no row in destination; "
                       f"left alone ({detail})", rows)

    # >1 row
    journal.record("outcome", asset_id, status=DUPLICATED,
                   reason=f"ambiguous ({detail}); destination shows {len(rows)} rows",
                   dep_rows=rows)
    return _result(asset_id, DUPLICATED,
                   f"ambiguous response and {len(rows)} rows in destination "
                   f"({detail})", rows)


def deploy_asset_with_precheck(asset_id, checksum, *, client=None, journal=None):
    """Fallback: deploy with a live per-asset pre-check GET (snapshot=None).

    Kept for single-asset / debugging use. The orchestrator does NOT use
    this -- a burst of these at concurrency 25 is what rate-limited us.
    """
    return deploy_asset(asset_id, checksum, client=client, journal=journal,
                        snapshot=None)


def redeploy(asset_id, checksum, *, client=None, journal=None):
    """Retry a logical write for an asset that came back UNCONFIRMED.

    Only ever uses the asset's ALREADY-BOUND key. If the journal has no
    binding, or a different one, we stop and ask -- never invent a key.
    """
    journal = journal or Journal()
    bound = journal.key_for(asset_id)
    expected = idempotency_key_for(asset_id)

    if bound is None:
        raise NeedsHumanDecision(
            f"redeploy({asset_id}): no key bound in the journal. A first "
            f"deploy_asset() call must have journalled a binding. Stop.")
    if bound != expected:
        raise NeedsHumanDecision(
            f"redeploy({asset_id}): journal has key {bound!r} but this build "
            f"would derive {expected!r}. Resending with a different key is "
            f"what created the as-0005 duplicates. Stop and decide.")

    # Same key, same path as deploy_asset from the pre-check on. The bound
    # key makes this safe: worst case the provider replays.
    journal.record("redeploy_attempt", asset_id, idempotency_key=bound)
    return deploy_asset(asset_id, checksum, client=client, journal=journal)


def _result(asset_id, status, reason, rows):
    return {"asset_id": asset_id, "status": status, "reason": reason,
            "dep_rows": rows}


def _safe_json(resp):
    try:
        return resp.json()
    except ValueError:
        return resp.text[:300]
