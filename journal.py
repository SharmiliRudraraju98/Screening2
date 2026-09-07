"""An append-only journal of deploy intentions and outcomes.

Every logical write is recorded here BEFORE the request leaves, so that if
the process dies mid-run we can reconstruct what was attempted and, more
importantly, which idempotency key was bound to which asset. Reusing that
exact key is the only safe way to retry; a new key makes a duplicate.

The journal is our record of intent. It is NOT evidence of what is in the
destination -- GET /s2/destination is the only authority on that. The two
are allowed to disagree, and reconciliation always trusts the destination.

File format: one JSON object per line (JSONL), append-only, never rewritten.
"""

import json
import threading
from datetime import datetime, timezone
from pathlib import Path

JOURNAL_PATH = Path(__file__).resolve().parent / "journal.jsonl"

# How we derive an asset's idempotency key. Stable and reproducible from the
# asset id alone, so a retry of the same logical write re-derives the same
# key without needing the journal -- the journal just records that we did.
KEY_PREFIX = "as-key-"


def idempotency_key_for(asset_id):
    """The one stable idempotency key for this asset. Pure function of the id."""
    return f"{KEY_PREFIX}{asset_id}"


class Journal:
    """Append-only. One instance per run; safe to share across a run."""

    def __init__(self, path=JOURNAL_PATH):
        self.path = Path(path)
        self._lock = threading.Lock()

    # -- writing ---------------------------------------------------------------

    def record(self, event, asset_id, **fields):
        """Append one event. `event` is a short verb; fields are free-form."""
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "asset_id": asset_id,
            **fields,
        }
        line = json.dumps(entry, ensure_ascii=False)
        with self._lock:
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        return entry

    def bind_key(self, asset_id, key):
        """Record the asset -> idempotency-key binding BEFORE any request.

        Idempotent in spirit: if we already bound a *different* key for this
        asset, that is a bug we must not paper over -- raise.
        """
        existing = self.key_for(asset_id)
        if existing is not None and existing != key:
            raise RuntimeError(
                f"asset {asset_id} already bound to key {existing!r}; "
                f"refusing to rebind to {key!r}")
        if existing is None:
            self.record("bind_key", asset_id, idempotency_key=key)
        return key

    # -- reading -------------------------------------------------------------

    def entries(self):
        """Yield every journal entry in order. Missing file -> nothing."""
        if not self.path.is_file():
            return
        for raw in self.path.read_text(encoding="utf-8").splitlines():
            raw = raw.strip()
            if raw:
                yield json.loads(raw)

    def key_for(self, asset_id):
        """The idempotency key previously bound to this asset, or None."""
        found = None
        for e in self.entries():
            if e.get("event") == "bind_key" and e.get("asset_id") == asset_id:
                found = e.get("idempotency_key")
        return found

    def outcomes(self):
        """asset_id -> list of outcome events, in order."""
        out = {}
        for e in self.entries():
            if e.get("event") == "outcome":
                out.setdefault(e["asset_id"], []).append(e)
        return out

    def last_outcome(self, asset_id):
        evs = self.outcomes().get(asset_id, [])
        return evs[-1] if evs else None
