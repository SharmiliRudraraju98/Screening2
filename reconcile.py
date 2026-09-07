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
from deploy import read_all_destination_rows
from journal import Journal

DESTINATION_PATH = "/s2/destination"


def destination_counts(client=None):
    """asset_id -> number of REAL dep rows in the destination.

    Rows are deduplicated by row id first (GET /s2/destination paginates
    with an overlapping window that repeats boundary rows verbatim).
    """
    client = client or HttpClient()
    counts = Counter(r["asset_id"] for r in read_all_destination_rows(client))
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
