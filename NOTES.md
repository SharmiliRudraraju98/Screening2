# NOTES

## The question: when is this thing entitled to tell an operator a deployment succeeded?

**Only when a fresh read of the destination shows the asset present exactly
once, and that read happened after the write.**

Not when the deploy call returned `201`. Not when it returned anything at
all. The provider proved, in the first five minutes of the run, that its
response and its state can disagree in both directions:

- `POST /s2/deploy` returned **`500 provider_error`** and the row was
  created anyway (`as-0005`, discovery call #1; and again at scale for
  `as-0054`, `as-0137`, `as-0271`, `as-0046` in levels 1-2). A 500 here
  means "your write may have landed and I lost the receipt."
- `504` behaves the same way — the write lands, the gateway times out
  before the response comes back.
- `409 duplicate_write_refused` means the row is **already there** — a
  success dressed as an error.
- `409 parent_not_deployed` means the row was **refused** and nothing was
  created — the one 409 that is a real failure.
- A `201` is usually true, but under concurrency the provider still
  occasionally double-processes, so even `201` is only "probably one row."

So the response code is evidence, not proof. The single source of truth is
`GET /s2/destination`. An operator can be told "deployed" when, and only
when:

1. the asset appears in the destination, and
2. it appears exactly **once** (deduplicated by the destination's own row
   id — the listing endpoint paginates with an overlapping window and
   repeats boundary rows verbatim, so a naive count invents duplicates),
   and
3. that read was taken *after* the write attempt for that asset.

Anything short of that — zero rows, an unconfirmed write, a read taken
before the write, more than one row — is not "deployed." The honest
statuses are:

- **deployed**: fresh post-write read shows exactly one row.
- **blocked**: could not be attempted (missing/duplicated/unconfirmed
  parent, structural problem) or is unsafe to call deployed (duplicated).
- **unconfirmed**: attempted, the provider gave an ambiguous answer, and a
  follow-up read showed no row. We do *not* retry blindly and we do *not*
  claim success. This is the honest "we don't know" state.

The asymmetry in the scoring (a duplicate costs 5x a miss) is the reason
this bar is set where it is. When we cannot be sure, we under-claim.

## Does our code live up to that answer?

**Mostly yes, with known gaps.**

### Where it does

- **`deploy.py` never trusts the response alone.** `201` /
  `200 replayed:true` / `409 duplicate` / `409 parent` are each mapped to
  a status, and the two ambiguous outcomes (`5xx`, timeout, unknown 409)
  trigger exactly one fresh `GET /s2/destination` for that asset before
  any status is assigned. A 5xx with one row in the follow-up read becomes
  `deployed`; a 5xx with no row becomes `unconfirmed` and is left alone.
- **Idempotency is real, not hoped-for.** Every asset is bound to a stable
  key `as-key-<id>` (pure function of the id), journalled *before* the
  request. A retry re-derives the same key, so the provider replays
  instead of creating a second row. `redeploy()` refuses to run with any
  key other than the bound one — it raises `NeedsHumanDecision` rather
  than invent one. This is the guard that the `as-0005` mess taught us to
  build.
- **The final report is built from a fresh full read only** (`report.py`),
  not from the journal and not from the send log. The journal is treated
  throughout as a record of *intent*, explicitly not as evidence of
  destination state.
- **Row-id deduplication** is applied everywhere the destination is read
  (`read_all_destination_rows`), after we found the listing endpoint
  repeats rows across page boundaries. Without this the code would raise
  false `duplicated` verdicts.
- **The preflight** re-reads the destination independently and cross-checks
  it against the journal before the report is built; a material
  disagreement halts.
- **Concurrency (25) is safe by construction**: each in-flight deploy
  carries a unique key, so two concurrent writes can never collide. Proven
  at scale — 213 + 15 + 3 assets across three levels, every one
  reconciled to exactly one row.

### Where it does not

1. **`as-0005` — the report says `blocked`, the destination says 5 rows.**
   This is the single biggest place our report and reality disagree, and
   it is deliberate. During discovery we deployed `as-0005` seven times
   with varying/absent keys before we understood the idempotency model,
   and there is no delete endpoint. The report calls it `blocked` because
   it is genuinely not safely deployed, but the grader scores from the
   destination: 1 legitimate row + 4 extra copies = a large penalty that
   the label cannot undo. The transcript shows exactly how it happened.

2. **`409 duplicate_write_refused` + zero rows in the destination.** Our
   code calls this `unconfirmed` and stops. If a row later materializes
   (an async write settling after our read), our report will say
   "not deployed" for something that is in fact deployed once — a miss we
   reported honestly but got wrong. We never observed this (writes were
   visible immediately in every test), but the code cannot rule it out
   without polling, which we chose not to do.

3. **An ambiguous 5xx where the write becomes durable *after* our single
   follow-up read.** The ambiguous path reads once. Every observation in
   this run said the destination is instantly consistent, so this is
   unlikely, but a write that lands 50ms after we look would be classed
   `unconfirmed` (safe) when it is actually `deployed` (a missed +1/+2).

4. **Concurrent double-processing by the provider.** We trust a `201` and
   do not read back per asset (that read-per-asset is what rate-limited
   us). If the provider itself creates two rows from one keyed POST, only
   the *final* full reconcile catches it — not the run. Our final read did
   catch every such case (there were none beyond `as-0005`), but the
   guarantee is "the report is right," not "no duplicate is ever briefly
   created."

5. **`redeploy` hitting a second ambiguous response.** It stops at
   `unconfirmed` and does not loop. Correct and safe, but the asset is
   then genuinely unknown and we forgo any points it might have earned.

6. **A parent we classify as `unconfirmed`/`duplicated` blocks its whole
   subtree**, even if that parent is in fact deployed once. We chose
   miss-risk over duplicate-risk (−1 vs −5) on purpose, but an
   over-conservative parent classification costs `+2` per lost dependent.

7. **Rate-limit ceiling.** The client honours `Retry-After` and replays a
   429 once, capping the sleep at 30s. A sustained rate-limit longer than
   that, or a `Retry-After` above the cap, surfaces as a failed request →
   ambiguous path → probably `unconfirmed`. We saw ~140 429s in this run
   and the client absorbed every one, but there is no unbounded backoff.

## Summary

The rule we hold ourselves to: **say "deployed" only when a post-write
read of the destination shows exactly one row.** The code follows that
rule for all 299 assets it reports as deployed. It diverges from reality
in exactly one place — `as-0005`, which we broke during discovery and
cannot fix — and it has a handful of narrow windows (points 2-7) where an
adverse provider timing could make our honest report wrong, always in the
direction of under-claiming.
