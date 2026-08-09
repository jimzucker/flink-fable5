# Phase 21 — SQL correctness: what we found, and the plan from here

**No AWS or Confluent runs until this plan is agreed.** Everything below through
"Findings" was measured on the local Docker rig at zero cost.

---

## Findings so far (local, controlled)

### 1. Ordering is CORRECT — on every axis a consumer cares about

17,572 published records, SQL with conflation on, read in offset order
(a key always hashes to one partition, so offset order IS per-key order):

| topic | as_of backwards | price backwards | keys ending stale |
|---|---|---|---|
| position-by-account-ticker | 0 | 0 | 0 |
| position-by-ticker | 0 | 0 | 0 |
| mv-by-account-ticker | 0 | 0 | 0 |
| mv-by-ticker | 0 | 0 | 0 |

* No position ever arrives out of order.
* **No out-of-order price is ever used to compute an output** — checked on every
  record, not just the last, so intermediate results are covered.
* No consumer is ever left holding a value older than one already sent to them.

Traders never see a number move backwards, and never see a value computed from a
stale tick.

### 2. The real defect: windows do not close when a symbol goes quiet

Clean A/B, identical input (2,260 trades / 4,520 prices, same seed), one variable:

| | exact | p50 | p99 | max |
|---|---|---|---|---|
| conflation OFF | **100%** | 0ms | 0ms | 0ms |
| conflation ON | 0% | 2,016ms | 3,458ms | 3,458ms |

The pipeline never *emits* a value using the final price. Everything it does
publish is correct and in order; the last update simply never happens.

**This is not an end-of-run artifact.** Any symbol that stops ticking has its
open window held indefinitely, so its market value freezes at the previous
window — a thinly-traded ticker during the trading day hits this, not just the
end of the stream. That makes it a production correctness fault: a trader is
left looking at a stale market value for as long as the symbol is quiet.

**Leading cause:** no source idle timeout locally. When a partition sees no data
the watermark stops advancing, so windows never close. `sql.source.idle.timeout.ms`
exists and is set to 5000 on AWS — but was never set on the local rig, which is
exactly where the staleness reproduced.

---

## Plan — local first, cloud only once this is settled

### Step 1 (local, free): does the idle timeout fix it?
Re-run the A/B with `sql.source.idle.timeout.ms` set (try 5000, then 1000).
*Prediction if the diagnosis is right:* staleness collapses toward 0 and the
six checks pass with conflation still ON.

### Step 2 (local, free): the quiet-symbol case, which is the real one
Step 1 only proves the tail flushes. Construct the production condition: keep a
few symbols ticking while others go silent mid-run, then measure staleness for
the silent ones specifically. This is the case that matters and we have never
tested it.

### Step 3 (local, free): confirm the fix costs nothing
Re-measure throughput locally with the idle timeout on. Closing windows on a
timer does more work when data is sparse; verify that is negligible.

### Step 4 (decide): only then consider a cloud run
A cloud run is only worth it to confirm the fix at realistic cardinality
(3,000 symbols, salted feed). One condition, ~$2, and only if steps 1-3 pass.

---

## What is NOT worth re-running

* Ordering — measured, clean, and not scale-dependent.
* The materializer — already ruled out as a cause in Phase 20.
* Throughput/scaling — settled in Phase 20 and not affected by this.

## Corrections carried into this phase

* "Late, not wrong" was too generous. At rest the value never corrects, so for a
  consumer it is simply wrong. The user's standard is the right one: final
  position and final market value must both be correct once ticks stop.
* The previous AWS Phase 21 run was invalid — the script recreated topics under
  an already-running job (`topics_recreate=true` after the app was RUNNING),
  which corrupted even the position checks. Fix the ordering before any rerun.
