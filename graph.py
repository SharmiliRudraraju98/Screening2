"""Dependency graph over the 300 approved assets.

Edges come ONLY from each asset's own `depends_on` field. A `null`
depends_on means no parent. Direction is never inferred from `kind` --
FINDINGS.md already showed a cta depending on a form, so kind tells us
nothing about who waits for whom.

What this module produces:

  - levels: level 0 = no parent; level N = every parent is at level < N,
    and at least one parent is at level N-1. This is a longest-path
    layering, so a child never sits above a parent.

  - blocked assets, with a reason:
      "missing parent: <id>"        depends_on points outside the 300
      "circular dependency: <ids>"  the asset sits on a dependency cycle
      "parent <id> is <status>"     a parent resolved blocked / unconfirmed
                                    / duplicated, so this asset cannot go

Blocking is transitive: if a parent is blocked for any reason, every
descendant is blocked too, automatically.

The graph also folds in what we already know from the journal / a live
destination read: an asset that is already `deployed` or `already_present`
is "decided" and never appears as pending work; an asset that is
`unconfirmed` or `duplicated` is treated like a blocked parent for the
purpose of its descendants (we will not build on an unsafe foundation).
"""

from collections import defaultdict, deque

# Terminal decision states an asset can already be in before planning.
DECIDED_OK = {"deployed", "already_present"}
# States that are "resolved" but poison their descendants.
DECIDED_BAD = {"unconfirmed", "duplicated", "blocked"}


class DependencyGraph:
    def __init__(self, assets, *, prior_status=None):
        """
        assets:        list of asset dicts with id / depends_on / kind ...
        prior_status:  optional {asset_id: status} from the journal or a
                       live destination reconcile. Statuses in DECIDED_OK
                       or DECIDED_BAD are respected; anything else ignored.
        """
        self.assets = {a["id"]: a for a in assets}
        self.prior_status = dict(prior_status or {})

        self.parent_of = {aid: a.get("depends_on") for aid, a in self.assets.items()}
        self.children_of = defaultdict(list)
        for aid, parent in self.parent_of.items():
            if parent is not None:
                self.children_of[parent].append(aid)

        self.blocked = {}          # asset_id -> reason string
        self.level = {}            # asset_id -> int (only for non-blocked)
        self.cycles = []           # list of lists of ids
        self.decided = {}          # asset_id -> status (from prior_status, OK only)

        self._classify()

    # -- building ------------------------------------------------------------

    def _classify(self):
        # 1. Assets already decided OK are pinned out of the pending set,
        #    but still occupy a level so their children can be layered.
        for aid, status in self.prior_status.items():
            if aid in self.assets and status in DECIDED_OK:
                self.decided[aid] = status

        # 2. Direct structural blocks: missing parent.
        for aid in self.assets:
            parent = self.parent_of[aid]
            if parent is not None and parent not in self.assets:
                self.blocked[aid] = f"missing parent: {parent}"

        # 3. Prior-status poison: an asset resolved BAD blocks itself.
        for aid, status in self.prior_status.items():
            if aid in self.assets and status in DECIDED_BAD and aid not in self.blocked:
                if status == "blocked":
                    self.blocked[aid] = "previously blocked"
                else:
                    self.blocked[aid] = f"prior status: {status}"

        # 4. Cycles. Any asset on a cycle (reachable via depends_on back to
        #    itself) is blocked "circular dependency".
        self._find_cycles()

        # 5. Transitive block propagation: if a parent is blocked, so is
        #    every descendant -- reason names the nearest blocked ancestor.
        self._propagate_blocks()

        # 6. Level assignment for everything not blocked.
        self._assign_levels()

    def _find_cycles(self):
        WHITE, GREY, BLACK = 0, 1, 2
        color = defaultdict(lambda: WHITE)
        stack = []

        def walk(node):
            color[node] = GREY
            stack.append(node)
            parent = self.parent_of.get(node)
            if parent in self.assets:  # ignore missing-parent here
                if color[parent] == GREY:
                    # found a cycle: slice the stack from parent to node
                    idx = stack.index(parent)
                    cycle = stack[idx:]
                    self.cycles.append(list(cycle))
                    for c in cycle:
                        self.blocked.setdefault(
                            c, f"circular dependency: {' -> '.join(cycle + [cycle[0]])}")
                elif color[parent] == WHITE:
                    walk(parent)
            stack.pop()
            color[node] = BLACK

        for aid in self.assets:
            if color[aid] == WHITE:
                walk(aid)

    def _propagate_blocks(self):
        # BFS out from every currently-blocked asset along children edges.
        queue = deque(self.blocked)
        while queue:
            aid = queue.popleft()
            for child in self.children_of.get(aid, []):
                if child not in self.blocked:
                    self.blocked[child] = f"parent {aid} is blocked ({self.blocked[aid]})"
                    queue.append(child)
        # Also: descendants of a prior BAD (unconfirmed/duplicated) asset.
        for aid, status in self.prior_status.items():
            if aid in self.assets and status in ("unconfirmed", "duplicated"):
                q = deque(self.children_of.get(aid, []))
                while q:
                    c = q.popleft()
                    if c not in self.blocked:
                        self.blocked[c] = f"parent {aid} is {status}"
                        q.extend(self.children_of.get(c, []))

    def _assign_levels(self):
        # Longest-path layering over the DAG of non-blocked assets.
        # An asset's level = 0 if no parent (or parent is decided-OK at
        # level 0-equivalent), else max(parent levels) + 1.
        pending = {aid for aid in self.assets if aid not in self.blocked}

        # Memoised recursion; the non-blocked subgraph is acyclic by now.
        computing = set()

        def lvl(aid):
            if aid in self.level:
                return self.level[aid]
            parent = self.parent_of.get(aid)
            if parent is None or parent not in self.assets:
                self.level[aid] = 0
                return 0
            if parent in self.blocked:
                # shouldn't happen (child would be blocked too), guard anyway
                self.level[aid] = 0
                return 0
            computing.add(aid)
            self.level[aid] = lvl(parent) + 1
            computing.discard(aid)
            return self.level[aid]

        for aid in pending:
            lvl(aid)

    # -- queries -----------------------------------------------------------

    def levels(self):
        """{level_int: [asset_id, ...]} for all non-blocked assets, sorted."""
        out = defaultdict(list)
        for aid, l in self.level.items():
            out[l].append(aid)
        return {l: sorted(out[l]) for l in sorted(out)}

    def deploy_order(self):
        """Non-blocked, not-yet-decided asset ids, in level then id order.

        Assets already decided OK are omitted -- they are done."""
        order = []
        for l, ids in self.levels().items():
            for aid in ids:
                if aid not in self.decided:
                    order.append(aid)
        return order

    def pending_by_level(self):
        """Like levels() but excluding already-decided-OK assets."""
        out = {}
        for l, ids in self.levels().items():
            rem = [a for a in ids if a not in self.decided]
            if rem:
                out[l] = rem
        return out

    def summary(self):
        lv = self.levels()
        widest = max(lv.items(), key=lambda kv: len(kv[1])) if lv else (None, [])
        missing = {a: r for a, r in self.blocked.items() if r.startswith("missing parent")}
        cyclic = {a: r for a, r in self.blocked.items() if r.startswith("circular")}
        return {
            "total_assets": len(self.assets),
            "level_count": len(lv),
            "widest_level": {"level": widest[0], "size": len(widest[1])},
            "blocked_total": len(self.blocked),
            "blocked_missing_parent": len(missing),
            "blocked_circular": len(cyclic),
            "cycles": self.cycles,
            "decided_ok": dict(self.decided),
            "pending_count": len(self.deploy_order()),
        }
