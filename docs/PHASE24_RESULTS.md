# Phase 24 — positions only: the floor, and a real ceiling

**Method that worked:** bisect the pipeline and measure with event time carried
source → sink. Everything before this measured instrumentation instead of data,
and every instrumentation path failed differently.

---

## The ceiling

SQL, positions-by-symbol only, 4 input partitions, parallelism 2, 1,000 symbols,
`delivery=none`, emit 500ms.

| requested | produced/s | ratio | latency | verdict |
|---|---|---|---|---|
| 200k | ~360,000 | 1.00–1.02 | −4s flat | keeping up |
| 400k | 762,784 | **0.86** | +8s/min | over capacity |
| 400k | 838,342 | **0.78** | +13s/min | over capacity |

Implied capacity from each over-capacity sample:

    762,784 x 0.86 = 656,000/s
    838,342 x 0.78 = 654,000/s

Two different input rates, same answer: **~655,000 trades/s**.

For scale, the full pipeline appeared to top out at ~4,000 rec/s before the
bisection — but that figure came from broken collectors and is not comparable.
What IS comparable: the same rig, measured properly, sustains 360,000/s with
latency flat.

## Configuration matrix

| Condition | Engine | Input partitions | Output partitions | Parallelism | Symbols | produced/s | published/s | ratio | Latency |
|---|---|---|---|---|---|---|---|---|---|
| positions-by-symbol | SQL | 4 | 4 | 2 | 1,000 | 360,000 | 3,150–3,518 | 1.01 | −4s flat |
| positions-by-symbol | SQL | 4 | 4 | 2 | 1,000 | 762,784 | 3,850 | 0.86 | +8s/min |
| positions-by-symbol | SQL | 4 | 4 | 2 | 1,000 | 838,342 | 4,200 | 0.78 | +13s/min |

## Output volume is decoupled from input

Published held at **3,150–4,200/s while input moved 360,000 → 838,000/s**.

Output is `keys x emissions per key per second`, confirmed independently on both
terms:

| symbols | emit interval | published/s | predicted |
|---|---|---|---|
| 100 | 2000ms | 53 | 50 |
| 100 | 500ms | 199 | 200 |
| 100 | 100ms | 196 | 1,000 (**does not hold**) |
| 1,000 | 500ms | 1,302 | 2,000 |

The model holds at 500ms and 2000ms and breaks below 500ms, where output floors
near 200/s for reasons not established. Not checkpointing — that was 10s and
would have shown ~10/s.

**More parallelism does not increase output.** Parallelism is not in
`keys x emit rate`, and utilization was ~3%. Parallelism raises INPUT capacity,
which is a separate ceiling.

## What the price path cost

Removing it took backpressure from 99.8–99.9% at the sources to **0.0%**, and
utilization from a pinned ~31% to 0–2.3%. The `SinkUpsertMaterializer` bottleneck
found in Phase 23 sat on the market-value path; removing that path removed the
constraint rather than relocating it.

## Measurement failures corrected in this phase

| path | how it failed |
|---|---|
| Flink `numRecordsOut` | 29,970/s against a 10,667/s generator; two collectors returned 29,970 and 0 |
| consumer-group offsets | commit only ON CHECKPOINT — read 0 with checkpointing off |
| `/backpressure` endpoint | 0.0% on a job that was 99.9% blocked |
| averaged busy% | 31% hid sources at 0.2% and the bottleneck at 85–93% |
| per-edge counters | a forking source counts each record once per output edge |
| `--from-beginning` | re-read the first records every sample, reporting STALLED on a healthy pipeline |

**The rule that came out of it:** measure source event time versus processing
time at write. Ratio answers "keeping up?", latency answers "how stale?", and a
count answers "how many?" — three questions, three measurements, none
substitutes for another.

## Changes shipped

* trade dedup removed from both engines (unique publisher + exactly-once)
* generator no longer injects duplicates; validator asserts uniqueness
* `positions.only` and `single.output` modes
* checkpointing mode follows the delivery guarantee
* `scripts/event_time_rate.py`, `rates.py`, `local_consumed.py`,
  `local_backpressure.py`, `local_subtask_trace.py`, `local_source_probe.py`

---

# Phase 24 — scaling and emit-cadence results

All local, SQL engine, positions-by-symbol only, 1,000 symbols,
`delivery=none`, checkpoint 60s. Each row is a 30s sample.

## Parallelism ceiling

| In parts | Out parts | Par | Emit | Requested | ratio | produced/s | published/s | Verdict |
|---|---|---|---|---|---|---|---|---|
| 4 | 4 | 2 | 500ms | 200k | 1.00–1.02 | 333k–400k | 3,150–3,917 | keeping up |
| 4 | 4 | 2 | 500ms | 400k | 0.73–0.86 | 713k–838k | 3,600–4,200 | over ceiling |
| 4 | 4 | 4 | 500ms | 400k | 0.98–1.01 | 799k–976k | 4,275–5,525 | keeping up |
| 4 | 4 | 4 | 500ms | 800k | 0.73–0.81 | 642k–710k | 4,700–5,651 | over ceiling |
| 8 | 8 | 8 | 500ms | 800k | 0.80–1.06 | 873k–1,075k | 4,368–5,906 | over ceiling |

**P=2 ≈ 550–650k/s. P=4 ≈ 950k/s. P=8 no better than P=4** — at 8 slots the
generator, Kafka and Flink contend for the same laptop cores, so P=8 is a floor
on the job's capacity, not a ceiling. A clean P=8 number needs the generator off
the box under test (pre-queue a backlog, then start Flink against static data).

**Capacity carries ~15% run-to-run variance.** The identical P=2 config measured
655k on one run and 559k on a repeat. Quote a range, never a point. The
*sustained* figure (highest produced/s at ratio >= 0.99) is directly observed and
more defensible than `produced x ratio`, which is an extrapolation that inherits
all of the generator's jitter.

## Emit interval sweep (P=2, 200k rung)

| Emit | Latency | published/s | ratio |
|---|---|---|---|
| 500ms | 1.0s | 3,500 | 1.00 |
| 250ms | 0.8s | 6,900 | 1.00 |
| 100ms | 0.6s | 12,200 | 1.00 |
| 50ms  | 0.5s | 22,200 | 1.00 |

**Sub-500ms works down to at least 50ms.** Output scales ~1/interval with no
cliff. Input capacity is untouched (ratio 1.00 across a 6.4x change in write
rate) — output cadence and input throughput are independent knobs.

Latency floors at ~0.5s: 100ms and 50ms are indistinguishable, so below ~100ms
the emit buffer has stopped being the binding constraint. **100ms is the pick** —
essentially all the latency benefit at half the writes of 50ms.

The previously-noted "~200/s output floor below 500ms" was an artifact of the old
collector and is **withdrawn**; output at 50ms is 22,000/s.

## Measurement bug found and fixed

`event_time_rate.py` stamped the wall clock *before* calling `newest_as_of()`,
which spawns `docker compose exec` plus a cold kafka-console-consumer JVM and
costs **4-8s** (measured). Every record it returned postdated that timestamp, so
healthy runs reported **-4.4s** of latency.

* Fixed by stamping the clock after each sample returns.
* Real latency is **~1.0s** at 500ms emit, not negative.
* **No capacity number changes** — a constant offset cancels in `b - a`, so every
  ratio remains valid. Only the latency column was wrong.
* Docker VM clock is within 0.5s of the host, so the earlier "clock skew"
  explanation was wrong and has been retracted.

## OPEN — correctness issue, blocks further performance work

Measured over one window at the 200k dial:

| Source | Rate |
|---|---|
| Generator's own counter (ground truth) | 216,327/s |
| Kafka `trades` end offsets (`produced/s`) | 260,000/s |

**Kafka holds ~18% more records than the generator reports sending.** The
generator honours its dial; the excess is on the Kafka side.

Leading hypothesis, NOT yet confirmed: the producer sets `acks=all` and
`linger.ms=5` but **no `enable.idempotence`** (`DataGenerator.java:145-146`), so a
retry after a partial failure appends the record twice. `tradesSent++` counts
send calls, not broker appends, which is why the generator would not see it.

Why this matters more than the throughput bias: **trade dedup was removed from
both engines** on the premise that upstream cannot send duplicates. If the
producer is in fact writing duplicates, positions are double-counting and the
aggregates are wrong.

Decisive test: count distinct `tradeId` against total records in the topic. IDs
are sequential (`T-<runId>-<8 digits>`), so duplicates are directly detectable.

Consequence for every number above: `produced/s` counts broker appends, so
absolute throughput is inflated by whatever this factor turns out to be.
Relative comparisons (P=2 vs P=4, the emit sweep) are unaffected — the bias
applies uniformly.

## Not done

* P=4 and P=8 were measured on the old (biased-latency) collector; their ratios
  are valid, their latency columns are not.
* P=2 vs P=4 at 100ms emit was started and killed before completing.
* Phase 22 (AWS/Confluent like-for-like) still not started.
