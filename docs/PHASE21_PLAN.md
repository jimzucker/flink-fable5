# Phase 21 — is SQL's market value correct, or just late?

**The observation.** On AWS, SQL produced **zero** exact market-value matches
across 1,200 keys in every run. ~93% landed inside a 2,000ms lag tolerance, ~3%
outside it. DataStream on the identical feed matched **945/1000 exactly**.

**The question.** Is SQL publishing *stale* values (a correctness defect), or
merely *late* ones that a bounded run truncates (a latency characteristic)?
Right now neither "SQL is correct" nor "SQL is incorrect" is established.

**What Phase 20 already ruled out.** The `SinkUpsertMaterializer` is NOT the
cause: removing it gave 20/1000 wrong, and the control with it enabled gave
30/1000 — worse. That causal claim was made from one arm and retracted.

---

## Method: measure the distribution, don't sweep the threshold

A 2s/5s/10s sweep only tells us which arbitrary line the failures fall behind.
Better: have the validator report **how stale each market value actually is**,
in milliseconds, and print the distribution.

For every MV key: `staleness = final_price_event_time − implied_price_event_time`
where `implied_price` is recovered from `mv / position`.

Report p50 / p90 / p99 / max, plus a count beyond several thresholds. One run
then answers the threshold question for every possible threshold at once, and
converts a binary pass/fail into a measurement.

**This also re-grades DataStream on the same axis.** "945/1000 exact" says its
staleness is ~0 for most keys; the distribution says what the tail does.

## Hypotheses, most likely first

### H1 — End-of-run truncation (an artifact, not a defect)
The generator stops, so the last prices for a symbol sit in a 250ms tumble
window that never closes — no later record advances the watermark past it. The
final MV then reflects the second-to-last price forever. **DataStream would not
show this** because it has no windowing on that path, which fits the observed
asymmetry exactly.
*Prediction:* staleness clusters near the conflation interval, and the failing
keys are disproportionately those whose last tick arrived near the end of the run.
*Fix if true:* it is a measurement artifact — send a watermark-advancing trailer,
or exclude the final window. Not a production defect.

### H2 — Idle-key starvation
Symbols in the Zipf tail get few ticks. If a key's last update predates the
final price by more than the window, its MV never refreshes.
*Prediction:* failing keys correlate with LOW tick counts.

### H3 — Genuine staleness under load
SQL's conflate + tumble + checkpoint path simply runs seconds behind and never
catches up while the backlog drains.
*Prediction:* staleness scales with backlog depth and is roughly uniform across
keys, not concentrated in the tail.

### H4 — Watermark stall on idle partitions
Known failure mode in this project (Phase 16: 119k/s consumed, zero written).
`sql.source.idle.timeout.ms` is set to 5000, so partly mitigated — but 5s is
longer than the 2s tolerance, which alone could explain the tail.
*Prediction:* staleness clusters near 5,000ms.

## Pre-declared interpretation (before any run)

* **p99 staleness < 2,000ms** → SQL is correct; the earlier failures were the
  tail of a latency distribution and the 2s threshold was simply too tight.
* **p99 between 2,000 and 10,000ms, with the mass near the conflate/idle
  interval** → SQL is *late, not wrong*. Report as a latency characteristic and
  quote the number.
* **A tail beyond 10,000ms, or staleness that grows without bound** → genuine
  correctness defect. Chase the mechanism.
* **Failing keys correlate with low tick counts** → H2, an idle-key problem,
  fixable with a periodic re-emit.

## Cost discipline

* One small run: 200 symbols, 16 partitions, P=8 — correctness needs distinct
  keys, not scale. ~$2.
* Validator change is exercised on the **local Docker rig first**; only the
  measurement needs AWS.
* Record to the ledger as it lands; tear down unconditionally.

## Method rules carried forward

* Calibrate any metric against a known truth before trusting it.
* A test that cannot fail is not evidence — both the static-price and
  whole-run-tolerance versions of this check were vacuous.
* Declare interpretation thresholds before the run, not after.
* One condition at a time, recorded before the next starts.
