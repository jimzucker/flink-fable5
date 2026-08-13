# Phase 25 — Latency baseline: generator → Kafka → consumer → Flink

**KPI: latency, measured from record creation to record consumption.**
Everything else (throughput, parallelism, utilization) is context for that one
number.

Deliberately run at **10,000 trades/s** — roughly 2% of the measured P=2 ceiling.
This is a latency phase, not a capacity phase. Load stays far below saturation so
that latency reflects the pipeline's inherent cost, not queueing behind a
backlog.

---

## The measurement

Every trade carries `created_ts`, stamped at construction in the generator. Every
consumer computes:

    latency = read_time − created_ts

Reported as **p50 / p90 / p99 / max**, never as a mean. A mean hides the tail,
and the tail is what a trader actually experiences.

**One tool, both steps.** The same `LatencyConsumer` is pointed at the *input*
topic in Step 1 and the *output* topic in Step 2. That makes the two directly
subtractable:

    Step 2 latency − Step 1 latency = what the Flink pipeline costs

That decomposition is the point of splitting the phase in two. Step 1 alone
answers "how much latency is Kafka and the harness?", and it is the floor no
pipeline can beat.

### Clock domain — a hard requirement

`created_ts` is stamped inside the generator container; `read_time` must be
stamped in the **same clock domain**. The consumer therefore runs **inside a
container on the same Docker VM**, never on the macOS host.

This is not hypothetical. Phase 24 spent effort on a latency figure of −4.4s that
turned out to be the collector's own JVM startup being attributed to the
pipeline. Rules taken from that:

* Stamp the read clock at the moment of the read, never before a blocking call.
* Host and Docker VM clocks were measured within 0.5s of each other, so skew is
  *not* the usual explanation for a weird latency — instrumentation is.
* A latency that is constant across samples is an offset, not a measurement.

---

## Step 1 — Generator and plain Kafka consumer

**Question: can we produce 10k/s across 4 partitions, and read it back keeping
up, at consumer parallelism 2 and 4? What is the floor latency?**

Config held constant: 4 partitions on `trades`, 1,000 symbols, 10,000 trades/s,
`qty.override=1` (so a position is a trade count — see validation in Step 2).

| Condition | Consumers in group | Partitions each |
|---|---|---|
| 1A | 2 | 2 |
| 1B | 4 | 1 |

Consumer parallelism here means **N consumer instances in one consumer group**,
so Kafka assigns partitions across them — the plain-consumer analogue of Flink
source parallelism.

### Pass criteria

1. **Rate** — Kafka end-offset delta on `trades` = 10,000/s ± 5%.
2. **Keeping up** — consumed/s = produced/s, and consumer lag flat or shrinking
   over a 60s window. Growing lag fails the condition.
3. **Latency** — p50/p90/p99/max reported. No threshold to pass; this run
   *defines* the floor.

### Uniqueness assertion (cheap, not a blocking investigation)

Phase 24 reported Kafka holding ~18% more records than the generator sent. **That
finding is retracted** — it compared the generator's counter, which prints only
every 10s, against a continuous broker measurement over a 49s wall window. That
window can span as little as ~40s of true report-to-report time, and at 40.8s the
generator rate works out to 259,804/s against the 260,000/s measured from
offsets. The gap closes entirely. It was quantization, not duplication.

Duplicates could only ever come from **producer retries** (broker appends, ack is
lost, producer resends). `enable.idempotence` defaults to **true** in Kafka
clients 3.0+ and is compatible with our `acks=all`, so the producer very likely
already rejects a repeated append.

Still worth asserting once, because it is nearly free at 10k/s and because the
"no dedup needed" premise depends on it:

* Read the whole `trades` topic after the run; compare **total records** against
  **distinct `tradeId`**. IDs are sequential (`T-<runId>-<8 digits>`), so repeats
  and gaps are both directly visible.
* Expected: distinct == total, sequence gap-free.

If duplicates ever do appear, the fix belongs on the **producer**
(`enable.idempotence=true`), not in the pipeline — prevent them at the source
rather than clean up downstream.

---

## Step 2 — Clean Flink pipeline, positions by symbol

**Question: what latency does the pipeline add, at P=2 and P=4, and is the
answer correct?**

Pipeline is the minimum that produces the deliverable: read `trades` → key by
symbol → running sum of qty → emit position by symbol. No prices, no market
value, no second output. `positions.only=true`, `single.output=true`.

| Condition | Flink parallelism | Input partitions |
|---|---|---|
| 2A | 2 | 4 |
| 2B | 4 | 4 |

`position.emit.interval.ms=0` (emit per update). Any emit interval adds up to its
own length of conflation delay — at 100ms that is ~50ms of average latency that
belongs to the config, not the pipeline. Measure the pipeline clean first; 100ms
can be measured afterward as the practical production setting.

Output records carry `created_ts` of the trade that produced them, so the same
`LatencyConsumer` reads `position-by-ticker` and reports the same percentiles.

### Correctness gate — runs before any latency number is reported

**Final position per symbol from the pipeline must equal the position computed
independently from what the generator sent.**

Procedure:

1. Run for a fixed window, then **stop the generator**.
2. Wait for drain (consumer lag → 0).
3. Compute expected: read the full `trades` topic, **sum `qty` per symbol**.
   With `qty.override=1` this is simply the trade count per symbol.
4. Compute actual: read `position-by-ticker`, take the **last record per key**.
5. Compare all 1,000 symbols. **Any mismatch fails the condition** and the
   latency number for that condition is not reported.

**Why a sum on one side and a last-value on the other.** The position *is* a sum,
but the output topic carries **running totals, not deltas** — each emitted record
holds the cumulative position as of that moment
(`TickerPositionAggregator.java:41-43`):

    long updated = (netQty.value() == null ? 0L : netQty.value()) + trade.qty;
    netQty.update(updated);
    TickerPosition snapshot = new TickerPosition(trade.ticker, updated, trade.eventTime);

So the final record for a symbol already contains its total. Summing the output
records would be wrong — that would add every intermediate snapshot together.
Take the last one and compare it against the independently-summed input.

This is the standing rule applied literally: correctness and completeness before
performance. A fast wrong number is worth nothing.

---

## Repeats

**One run per condition, 2 minutes of measurement.** Four conditions, ~20 minutes
total including container start and drain.

Caveat to carry into the reporting: Phase 24 measured **~15% run-to-run variance**
on identical config with fresh containers. A single run therefore gives a latency
figure with no error bar. That is an acceptable trade for a first pass — get the
shape of the answer cheaply, and add repeats only where the single run leaves a
question. Do not quote these as precise until they have been repeated.

---

## Deliverable — the status table

Printed after **each condition completes**, not only at the end.

    Phase 25 — held constant: SQL | local | 4 input partitions
                              10,000 trades/s | emit 0ms | delivery=none | 2 min window

| # | What | Par | Symbols | Accounts | in trades/s | read/s | Lag | **p50** | **p90** | **p99** | **max** | BackPressure | Correct | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1A | plain consumer | 2 | 1,000 | 5 | | | | | | | | n/a | n/a | |
| 1B | plain consumer | 4 | 1,000 | 5 | | | | | | | | n/a | n/a | |
| 2A | Flink positions | 2 | 1,000 | 5 | | | | | | | | | PASS/FAIL | |
| 2B | Flink positions | 4 | 1,000 | 5 | | | | | | | | | PASS/FAIL | |

Plus the derived line that is the actual finding:

    pipeline cost = Step 2 p99 − Step 1 p99, at P=2 and at P=4

### Column notes

* **Latency in ms**, not seconds — at 10k/s with no conflation these land in tens
  of ms, and seconds would round everything to 0.0.
* **p99 is the KPI.** p50 is comfort, max is the outlier hunt, p99 is the number
  worth committing to.
* **Symbols** is the real cardinality knob: it sets keyed-state size and the
  number of distinct output keys, so it drives Step 2's work and the size of the
  correctness comparison.
* **Accounts** does not affect output cardinality in this phase — positions are
  emitted by symbol only, so the account string just rides along on each trade.
  Carried for provenance and cross-phase comparison; it would matter again if
  position-by-account-symbol were re-enabled.
* **`read/s` vs `in trades/s`** — divergence invalidates the row no matter how
  good the latency looks. Lag confirms it independently.
* **Correct** prints `PASS n/1000`, never a bare PASS. A mismatch prints the
  first 5 offending symbols with expected vs actual.
* **Dropped from `RUN_CHECKLIST.md`:** `in prices/s` (prices off), `out MV/s` (no
  market-value path), and all cost/KPU columns (local: identical on every row).

### Failure rendering

A failed condition prints in place rather than being dropped:

    | 2A | Flink positions | 2 | 1,000 | 5 | 10,008 | 6,240 | GROWING | — | — | — | — | 92.1% | FAIL | NOT KEEPING UP
         FAIL: 12 symbols mismatched. AAPL expected 1,204 got 1,198 (-6) ...
         Latency suppressed: not reported for a condition that failed correctness.

Latency is never printed for a condition that failed its correctness gate.

### Validity gate — lag must be measurable

**Flink commits consumer-group offsets only ON CHECKPOINT.** With checkpointing
disabled, lag reads 0 and a drowning job looks perfect. Step 2 therefore runs with
checkpointing enabled (60s is fine — its only job here is to commit offsets).
Step 1 is unaffected: the plain consumer controls its own commits.

---

## What this phase does NOT do

* No capacity or ceiling testing. Load is fixed at 10k/s throughout.
* No AWS, no Confluent. Local only, per the standing rule that nothing goes to
  cloud without a proven local baseline.
* No DataStream-vs-SQL comparison. One engine, isolated variables.
* No P=8. Phase 24 showed the laptop, not Flink, is the constraint there.

## Standing constraints carried in

* Auto tear down when tests finish; never leave anything running.
* One condition at a time, recorded.
* Verify correctness and completeness before chasing performance.
* Record the phase prompt in `prompts/phase25_prompt.txt`.
