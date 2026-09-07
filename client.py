"""A thin HTTP client for the CQ screen provider.

Responsibilities, and nothing more:
  - read base URL and candidate key from the environment (via config.py);
  - send X-Candidate-Key on every request;
  - send a browser-like User-Agent (Cloudflare rejects some defaults);
  - apply a request timeout;
  - append one JSON line per request/response pair to transcript.jsonl,
    with the candidate key redacted anywhere it appears in a header dump;
  - stay authenticated: hold the short-lived bearer token, refresh it
    before it expires, and on a mid-request 401 refresh once and replay
    that same request with the new token.

The token dance is INFRASTRUCTURE. It exists so a request reaches the
server authenticated; it says nothing about whether the server accepted
the write. It is deliberately the only automatic replay in this file.

Explicitly NOT in scope: business retries. A write that fails, times out,
or returns an ambiguous non-401 status is reported to the caller as-is.
Whether to retry a write, and how to make that safe, is a decision for a
higher layer.
"""

import json
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

from config import CQ_BASE_URL, CQ_CANDIDATE_KEY

# A normal desktop Chrome UA. Anything browser-shaped clears Cloudflare;
# Python's urllib default does not.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT = 30  # seconds, per request
AUTH_START_PATH = "/auth/start"
# Refresh the token when this much of its life (or less) is left. The token
# lives ~300s; 60s of slack comfortably covers a slow request plus clock skew.
TOKEN_REFRESH_SLACK = 60  # seconds

TRANSCRIPT_PATH = Path(__file__).resolve().parent / "transcript.jsonl"

_REDACTED = "***REDACTED***"


def _redact(text):
    """Remove the candidate key from any string before it is written out."""
    if text is None:
        return None
    if not isinstance(text, str):
        text = str(text)
    return text.replace(CQ_CANDIDATE_KEY, _REDACTED)


def _redact_headers(headers):
    """Return a plain dict of headers with the key redacted from every value."""
    out = {}
    for name, value in dict(headers or {}).items():
        # Redact the auth-ish headers by name (so a rename of the key
        # constant can't leak it), and everything else by value match.
        if name.lower() in ("x-candidate-key", "authorization"):
            out[name] = _REDACTED
        else:
            out[name] = _redact(value)
    return out


def _body_preview(content, limit=4096):
    """Decode a response/request body to text for the transcript, truncated."""
    if content is None:
        return None
    if isinstance(content, bytes):
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            return f"<{len(content)} bytes, non-utf8>"
    else:
        text = str(content)
    text = _redact(text)
    if len(text) > limit:
        return text[:limit] + f"... <+{len(text) - limit} chars>"
    return text


class AuthError(RuntimeError):
    """Raised when the client cannot obtain or refresh a bearer token."""


class HttpClient:
    """One session, one transcript, one bearer token. Not thread-safe for
    concurrent requests; the token lock only guards refresh races."""

    def __init__(self, base_url=CQ_BASE_URL, key=CQ_CANDIDATE_KEY,
                 timeout=DEFAULT_TIMEOUT, transcript_path=TRANSCRIPT_PATH):
        self.base_url = base_url.rstrip("/")
        self.key = key
        self.timeout = timeout
        self.transcript_path = Path(transcript_path)
        self._session = requests.Session()
        self._session.headers.update({
            "X-Candidate-Key": key,
            "User-Agent": USER_AGENT,
        })
        # Token state, guarded by _token_lock.
        self._token = None
        self._token_expires_at = 0.0  # monotonic seconds
        self._token_lock = threading.Lock()
        self.last_run_minutes_remaining = None

    # -- auth (infrastructure) --------------------------------------------

    def _fresh_token(self, *, force=False):
        """Return a currently-valid bearer token, refreshing if needed.

        Not a business retry: this only ensures the next request carries a
        live token. It talks to POST /auth/start, which is not itself a
        write against the marketer's account.
        """
        now = time.monotonic()
        with self._token_lock:
            still_good = (
                self._token is not None
                and not force
                and now < self._token_expires_at - TOKEN_REFRESH_SLACK
            )
            if still_good:
                return self._token
            self._token = None  # drop the stale one before we try to replace it
            data = self._auth_start()
            token = data.get("access_token")
            if not token:
                raise AuthError(f"/auth/start returned no access_token: {data!r}")
            expires_in = float(data.get("expires_in", 300))
            self._token = token
            self._token_expires_at = time.monotonic() + expires_in
            if "run_minutes_remaining" in data:
                self.last_run_minutes_remaining = data["run_minutes_remaining"]
            return self._token

    def _auth_start(self):
        """One POST /auth/start. Logged like any other request. No replay."""
        resp = self._send_once("POST", f"{self.base_url}{AUTH_START_PATH}",
                               json_body=None, params=None,
                               extra_headers=None, timeout=self.timeout,
                               note="auth")
        if resp.status_code != 200:
            raise AuthError(
                f"/auth/start -> {resp.status_code} {resp.reason}: {resp.text[:200]}"
            )
        try:
            return resp.json()
        except ValueError as exc:
            raise AuthError(f"/auth/start body not JSON: {resp.text[:200]}") from exc

    # -- public API -----------------------------------------------------------

    def request(self, method, path, *, json_body=None, params=None,
                headers=None, timeout=None):
        """Send one request, authenticated. Returns the requests.Response.

        Automatic behaviour, all of it infrastructure -- it exists so the
        request reaches the server and is not turned away for a reason that
        has nothing to do with whether the write is acceptable:

          - attach a live bearer token, refreshing proactively near expiry;
          - if the server still answers 401, refresh once and replay this
            exact request a single time;
          - if the server answers 429, read Retry-After, sleep that long,
            replay this exact request once. A second 429 is logged and
            returned as-is -- no unbounded loop.

        Callers (deploy.py, the orchestrator) never see a 401 or a 429;
        they see a slow response or a genuine error. No other status
        triggers a replay. Transport failures (timeout, connection error)
        are logged and raised, not retried.
        """
        url = path if path.startswith("http") else f"{self.base_url}/{path.lstrip('/')}"
        eff_timeout = self.timeout if timeout is None else timeout

        def send(note):
            token = self._fresh_token()
            return self._send_once(
                method, url, json_body=json_body, params=params,
                extra_headers={**(headers or {}),
                               "Authorization": f"Bearer {token}"},
                timeout=eff_timeout, note=note)

        resp = send("request")

        if resp.status_code == 401:
            # Token rejected mid-run (expired early, rotated server-side).
            # Refresh and replay this one request exactly once.
            self._fresh_token(force=True)
            resp = send("request:auth-replay")

        if resp.status_code == 429:
            # Rate limited. Honor Retry-After and replay exactly once.
            delay = self._retry_after_seconds(resp)
            time.sleep(delay)
            resp = send("request:429-replay")
            if resp.status_code == 429:
                # Still limited. Do not loop. Hand it back; the caller's
                # ambiguous path (or its own error handling) takes over.
                self._append_note(
                    "rate_limited_after_replay",
                    url=url, method=method.upper(),
                    slept=delay)

        return resp

    @staticmethod
    def _retry_after_seconds(resp, *, default=10, cap=30):
        """Parse Retry-After (seconds form). Clamp to a sane range."""
        raw = resp.headers.get("Retry-After", "")
        try:
            secs = int(float(raw))
        except (TypeError, ValueError):
            secs = default
        return max(1, min(secs, cap))

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def post(self, path, **kw):
        return self.request("POST", path, **kw)

    def put(self, path, **kw):
        return self.request("PUT", path, **kw)

    def delete(self, path, **kw):
        return self.request("DELETE", path, **kw)

    # -- internals ----------------------------------------------------------

    def _send_once(self, method, url, *, json_body, params, extra_headers,
                   timeout, note):
        """Exactly one HTTP round trip, logged. Never retries anything."""
        method = method.upper()
        req_id = uuid.uuid4().hex[:12]
        merged_headers = {**self._session.headers, **(extra_headers or {})}

        record = {
            "id": req_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "note": note,
            "request": {
                "method": method,
                "url": url,
                "params": params,
                "headers": _redact_headers(merged_headers),
                "body": _body_preview(
                    json.dumps(json_body) if json_body is not None else None),
            },
        }

        started = time.monotonic()
        try:
            resp = self._session.request(
                method, url,
                json=json_body, params=params, headers=extra_headers,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            record["elapsed_ms"] = round((time.monotonic() - started) * 1000)
            record["error"] = {
                "type": type(exc).__name__,
                "message": _redact(str(exc)),
            }
            self._append(record)
            raise

        record["elapsed_ms"] = round((time.monotonic() - started) * 1000)
        record["response"] = {
            "status": resp.status_code,
            "reason": resp.reason,
            "headers": _redact_headers(resp.headers),
            "body": _body_preview(resp.content),
        }
        self._append(record)
        return resp

    def _append(self, record):
        line = json.dumps(record, ensure_ascii=False)
        with self.transcript_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def _append_note(self, note, **fields):
        """A bare marker line in the transcript, not tied to one round trip."""
        self._append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "note": note,
            **{k: _redact(v) if isinstance(v, str) else v
               for k, v in fields.items()},
        })


if __name__ == "__main__":
    # Smoke test. This IS an authenticated request and starts / spends the
    # run clock, so invoke it deliberately.
    import sys

    client = HttpClient()
    target = sys.argv[1] if len(sys.argv) > 1 else "/s2/assets"
    r = client.get(target)
    print(f"GET {r.url} -> {r.status_code} {r.reason} ({len(r.content)} bytes)")
    print(f"run_minutes_remaining seen at auth: "
          f"{client.last_run_minutes_remaining}")
    print(r.text[:1000])
