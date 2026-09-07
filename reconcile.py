"""Read the live destination and turn it into per-asset prior status.

The destination is the only authority on what is deployed. This function
reads it (paging if needed), counts rows per asset_id, and maps:

    0 rows  -> not present here (no entry returned)
    1 row   -> "already_present"
    >1 rows -> "duplicated"

The journal can add "unconfirmed" for assets we attempted and got an
ambiguous answer for, where the destination still shows nothing.
"""

from collections import Counter

from client import HttpClient
from journal import Journal

DESTINATION_PATH = "/s2/destination"


def destination_counts(client=None):
    """asset_id -> number of dep rows currently in the destination."""
    client = client or HttpClient()
    counts = Counter()
    cursor = None
    while True:
        params = {"cursor": cursor} if cursor is not None else None
        resp = client.get(DESTINATION_PATH, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"GET {DESTINATION_PATH} -> {resp.status_code}: {resp.text[:200]}")
        data = resp.json()
        for row in data.get("items", []):
            counts[row["asset_id"]] += 1
        cursor = data.get("next_cursor")
        if not cursor:
            break
    return dict(counts)


def prior_status(client=None, journal=None):
    """{asset_id: status} combining the live destination and the journal."""
    client = client or HttpClient()
    journal = journal or Journal()

    counts = destination_counts(client)
    status = {}
    for aid, n in counts.items():
        status[aid] = "already_present" if n == 1 else "duplicated"

    # Journal: assets we tried, got ambiguity, and destination shows nothing.
    for aid, evs in journal.outcomes().items():
        last = evs[-1]
        if aid in status:
            continue  # destination already speaks for it
        if last.get("status") == "unconfirmed":
            status[aid] = "unconfirmed"

    return status, counts


if __name__ == "__main__":
    import json
    st, counts = prior_status()
    print("destination row counts:", json.dumps(counts, indent=2))
    print("prior status:", json.dumps(st, indent=2))
