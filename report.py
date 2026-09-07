"""Build the /s2/report payload from a fresh destination read only.

Not from the journal, not from the send log. The score comes from what is
in the destination, so the report is built from the same source.

Rule set for this run:
  - as-0005          -> blocked, "duplicated during discovery phase, no
                        delete endpoint available"
  - every other asset (all 299) -> deployed

Sanity checks before it will emit:
  - exactly 300 assets in the report, matching the approved list
  - every 'deployed' asset has exactly one real row in the fresh read
  - every 'blocked' asset is genuinely not-exactly-once in the fresh read
  - no rows in the destination for asset ids outside the approved 300
"""

import json

from client import HttpClient
from deploy import read_all_destination_rows

ASSETS_SNAPSHOT = "assets_snapshot.json"
AS_0005_REASON = ("duplicated during discovery phase, "
                  "no delete endpoint available")


def build_report(client=None):
    client = client or HttpClient()
    approved = {a["id"] for a in json.load(open(ASSETS_SNAPSHOT))}

    rows = read_all_destination_rows(client)          # deduped by row id
    counts = {}
    for r in rows:
        counts[r["asset_id"]] = counts.get(r["asset_id"], 0) + 1

    unknown = sorted(set(counts) - approved)
    if unknown:
        raise RuntimeError(f"destination has rows for non-approved ids: {unknown}")

    report = {}
    for aid in sorted(approved):
        n = counts.get(aid, 0)
        if aid == "as-0005":
            report[aid] = {"status": "blocked", "reason": AS_0005_REASON}
        else:
            report[aid] = {"status": "deployed"}

    # --- sanity ---
    assert len(report) == 300, f"report has {len(report)} entries, expected 300"

    for aid, entry in report.items():
        n = counts.get(aid, 0)
        if entry["status"] == "deployed" and n != 1:
            raise RuntimeError(
                f"{aid} marked deployed but fresh read shows {n} rows")
        if entry["status"] == "blocked" and n == 1:
            raise RuntimeError(
                f"{aid} marked blocked but fresh read shows exactly 1 row")

    meta = {
        "total": len(report),
        "deployed": sum(1 for e in report.values() if e["status"] == "deployed"),
        "blocked": sum(1 for e in report.values() if e["status"] == "blocked"),
        "fresh_read_rows": len(rows),
        "duplicated_in_read": {a: c for a, c in counts.items() if c > 1},
        "missing_in_read": sorted(a for a in approved if counts.get(a, 0) == 0),
    }
    return {"report": report}, meta


if __name__ == "__main__":
    payload, meta = build_report()
    print(json.dumps(meta, indent=2))
    print()
    print(json.dumps(payload, indent=2))
    with open("report_payload.json", "w") as fh:
        json.dump(payload, fh, indent=2)
    print("\nwrote report_payload.json (NOT submitted)")
