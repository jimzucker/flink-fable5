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
| `--from-beginning` | re-read the first records每 sample, reporting STALLED on a healthy pipeline |

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
