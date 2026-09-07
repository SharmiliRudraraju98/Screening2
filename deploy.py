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


def _destination_rows(client, asset_id):
    """All destination rows whose asset_id matches. Pages if needed."""
    rows = []
    cursor = None
    while True:
        params = {"cursor": cursor} if cursor is not None else None
        resp = client.get(DESTINATION_PATH, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"GET {DESTINATION_PATH} -> {resp.status_code} {resp.reason}: "
                f"{resp.text[:200]}")
        data = resp.json()
        rows.extend(r for r in data.get("items", [])
                    if r.get("asset_id") == asset_id)
        cursor = data.get("next_cursor")
        if not cursor:
            break
    return rows


def deploy_asset(asset_id, checksum, *, client=None, journal=None):
    """Run the full decision for one asset. Returns a result dict:

        {"asset_id", "status", "reason", "dep_rows": [...]}

    status is one of DEPLOYED / ALREADY_PRESENT / UNCONFIRMED / DUPLICATED.
    Raises NeedsHumanDecision if a safe path forward requires a human.
    """
    client = client or HttpClient()
    journal = journal or Journal()

    # 1. Bind + journal the key BEFORE any request.
    key = idempotency_key_for(asset_id)
    journal.bind_key(asset_id, key)

    # 2. Pre-check the destination.
    rows = _destination_rows(client, asset_id)
    if len(rows) > 1:
        journal.record("outcome", asset_id, status=DUPLICATED,
                       reason="pre-check found >1 row", dep_rows=rows)
        return _result(asset_id, DUPLICATED,
                       f"{len(rows)} rows already in destination", rows)
    if len(rows) == 1:
        journal.record("outcome", asset_id, status=ALREADY_PRESENT,
                       reason="pre-check found existing row", dep_rows=rows)
        return _result(asset_id, ALREADY_PRESENT,
                       "row already in destination before deploy", rows)

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

    status_code = resp.status_code
    body = _safe_json(resp)

    if status_code == 201:
        rows = _destination_rows(client, asset_id)
        if len(rows) > 1:
            journal.record("outcome", asset_id, status=DUPLICATED,
                           reason="201 but destination shows >1 row",
                           dep_rows=rows)
            return _result(asset_id, DUPLICATED,
                           f"201 received but {len(rows)} rows present", rows)
        journal.record("outcome", asset_id, status=DEPLOYED,
                       reason="201 created", http=201, dep_rows=rows)
        return _result(asset_id, DEPLOYED, "201 created", rows)

    if status_code == 200 and isinstance(body, dict) and body.get("replayed"):
        rows = _destination_rows(client, asset_id)
        journal.record("outcome", asset_id, status=ALREADY_PRESENT,
                       reason="200 replayed:true", http=200, dep_rows=rows)
        return _result(asset_id, ALREADY_PRESENT,
                       "200 replayed:true -- provider made no new row", rows)

    if status_code == 409:
        err = body.get("error") if isinstance(body, dict) else None
        rows = _destination_rows(client, asset_id)

        if err == ERR_PARENT:
            # The write was REFUSED. Nothing was created. This is not a
            # duplicate; the parent named in the body is not deployed yet.
            missing = body.get("depends_on") if isinstance(body, dict) else None
            journal.record("outcome", asset_id, status=BLOCKED,
                           reason=f"409 parent_not_deployed ({missing})",
                           http=409, body=body, dep_rows=rows)
            return _result(asset_id, BLOCKED,
                           f"parent {missing} not deployed", rows)

        if err == ERR_DUPLICATE:
            # The write already landed on an earlier attempt.
            if len(rows) > 1:
                journal.record("outcome", asset_id, status=DUPLICATED,
                               reason="409 duplicate but >1 row", dep_rows=rows)
                return _result(asset_id, DUPLICATED,
                               f"409 duplicate and {len(rows)} rows present", rows)
            if len(rows) == 0:
                # Provider says duplicate, destination says nothing. Do not
                # guess -- this contradiction needs a human.
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

        # Some other 409 we have not seen. Treat as ambiguous, do not guess.
        return _ambiguous(asset_id, client, journal, key,
                          f"HTTP 409 unknown error: {body!r}")

    # 500 / unexpected 4xx / anything else: ambiguous. Read once, decide.
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
