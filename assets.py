"""Fetch the full approved-asset list and check its dependency graph.

Reading is idempotent, so this module retries a failed page fetch a few
times with backoff. That is a read convenience and has nothing to do with
write-retry policy, which lives elsewhere and does not exist yet.
"""

import time

from client import HttpClient

ASSETS_PATH = "/s2/assets"
_PAGE_FETCH_ATTEMPTS = 4


def fetch_all_assets(client=None, *, verbose=False):
    """Page through /s2/assets via next_cursor. Return list of asset dicts,
    deduplicated by id.

    The endpoint's paging window overlaps at some cursors: a handful of
    boundary rows come back on two consecutive pages, byte-identical. We
    collapse those by id. If two rows ever share an id but differ in
    content, that is a real conflict and we raise rather than guess.
    """
    client = client or HttpClient()
    by_id = {}
    raw_rows = 0
    duplicate_rows = 0
    cursor = None
    expected_total = None

    while True:
        params = {"cursor": cursor} if cursor is not None else None
        data = _get_page(client, params)

        page = data.get("items", [])
        for asset in page:
            raw_rows += 1
            existing = by_id.get(asset["id"])
            if existing is not None:
                duplicate_rows += 1
                if existing != asset:
                    raise RuntimeError(
                        f"asset {asset['id']} returned twice with different "
                        f"content: {existing!r} vs {asset!r}")
                continue
            by_id[asset["id"]] = asset

        expected_total = data.get("total", expected_total)
        cursor = data.get("next_cursor")

        if not cursor or not page:
            break

    items = list(by_id.values())
    if verbose:
        print(f"paged {raw_rows} rows, {duplicate_rows} were duplicates, "
              f"{len(items)} unique")
    if expected_total is not None and len(items) != expected_total:
        print(f"WARNING: {len(items)} unique assets but total says {expected_total}")

    return items


def _get_page(client, params):
    last_exc = None
    for attempt in range(1, _PAGE_FETCH_ATTEMPTS + 1):
        try:
            resp = client.get(ASSETS_PATH, params=params)
            if resp.status_code == 200:
                return resp.json()
            last_exc = RuntimeError(
                f"{ASSETS_PATH} -> {resp.status_code} {resp.reason}: {resp.text[:200]}")
        except Exception as exc:  # transport failure
            last_exc = exc
        if attempt < _PAGE_FETCH_ATTEMPTS:
            time.sleep(min(2 ** attempt, 8))
    raise last_exc


def check_dependencies(items):
    """Return a report dict about the depends_on graph."""
    ids = {a["id"] for a in items}
    with_dep = [a for a in items if a.get("depends_on") is not None]
    dangling = [
        (a["id"], a["depends_on"])
        for a in with_dep
        if a["depends_on"] not in ids
    ]
    kinds = {}
    for a in items:
        kinds[a.get("kind", "<none>")] = kinds.get(a.get("kind", "<none>"), 0) + 1

    return {
        "total_retrieved": len(items),
        "unique_ids": len(ids),
        "with_non_null_depends_on": len(with_dep),
        "dependency_edges": [(a["id"], a["depends_on"]) for a in with_dep],
        "dangling_dependencies": dangling,
        "kinds": kinds,
    }


if __name__ == "__main__":
    import json

    client = HttpClient()
    assets = fetch_all_assets(client)
    report = check_dependencies(assets)

    print(f"run_minutes_remaining (at auth): {client.last_run_minutes_remaining}")
    print(f"total retrieved:           {report['total_retrieved']}")
    print(f"unique ids:                {report['unique_ids']}")
    print(f"non-null depends_on:       {report['with_non_null_depends_on']}")
    print(f"kinds:                     {report['kinds']}")
    print()
    print("dependency edges (child -> parent):")
    for child, parent in report["dependency_edges"]:
        print(f"  {child} -> {parent}")
    print()
    if report["dangling_dependencies"]:
        print("!!! DANGLING (depends_on points to a missing id):")
        for child, parent in report["dangling_dependencies"]:
            print(f"  {child} -> {parent}  (MISSING)")
    else:
        print("OK: every depends_on points to an id present in the 300.")

    with open("assets_snapshot.json", "w") as fh:
        json.dump(assets, fh, indent=2)
    print("\nwrote assets_snapshot.json")
