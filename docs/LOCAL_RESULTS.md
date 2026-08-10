# Local (Docker) results — correctness and conflation behaviour

Same layout as the AWS status table, adapted: there is no KPU or partition
billing locally, so the cost columns are replaced by the laptop resources
actually consumed. **Every run below cost $0.**

Rig: 8 cores / 16 GB laptop, Docker limited to 8 CPUs / 7.6 GB. Kafka +
JobManager + TaskManager + generator + Prometheus + Grafana. 4 topic partitions,
4 task slots.

**Local numbers are for CORRECTNESS ONLY.** Throughput here is not comparable to
AWS and must never be mixed into the platform comparison — one machine, few
partitions, no MSK.

---

## Input vs output volume — defaults, 150s run

100 symbols @ 2,000 prices/s + 200 trades/s, parallelism 4, on the recommended
configuration (`sql.price.conflate.ms=0`, `mv.emit.interval.ms=1000`,
`position.emit.interval.ms=500`).

| | records | rate | keys |
|---|---|---|---|
| **IN — prices** | 402,000 | 2,680/s | 100 symbols |
| **IN — trades** | 40,200 | 268/s | — |
| **OUT — position by account+ticker** | 32,067 | 214/s | 500 |
| **OUT — position by ticker** | 19,094 | 127/s | 100 |
| **OUT — market value by account+ticker** | 86,486 | 577/s | 500 |
| **OUT — market value by ticker** | 19,517 | 130/s | 100 |
| **TOTAL in / out** | **442,200 / 157,164** | | |

**~2.8:1 reduction.** Output volume is what drives Kafka write cost, so this is
the number that maps to the bill -- not the input rate.

Market-value-by-account is the largest output (86,486) because one price tick
fans out to every account holding that ticker: 5 accounts x 100 symbols = 500
keys, each updating on its own timer.

Result on this run: **100% exact market values, 0ms staleness (p50/p90/p99/max),
0 ordering violations on all four topics, all six checks PASS.**

## Correctness status

Two configurations, and only one of them has a problem.

| | defaults / the fix<br>(output conflation) | input conflation 250ms<br>(what the AWS benchmarks ran) |
|---|---|---|
| Final price published per symbol | **correct** | correct |
| Final position per key | **correct** (0 mismatched) | correct (0 mismatched) |
| Final market value uses the final price | **correct — 100% exact** | **STALE — p50 ~2.9s** |
| Ordering of positions to consumers | **correct** (0 violations) | correct (0 violations) |
| Ordering of prices used for output | **correct** (0 violations) | correct (0 violations) |
| Consumers left on a superseded value | **none** (0 keys end stale) | none (0 keys end stale) |
| Six-check validation | **PASS** | FAIL (2 checks) |

**On the code defaults there is no outstanding correctness defect.** Every row
passes, including at 400,000 prices.

The FAIL column exists only because the Phase 20 benchmark scripts overrode the
defaults with `sql.price.conflate.ms=250` and `mv.emit.interval.ms=0` -- input
windowing ON and output rate limiting OFF, backwards on both counts. That
configuration is dominated: it publishes 3x MORE records than the defaults AND
is stale. It should not be used, and the terraform variable now documents why.

---

## Dense — the realistic shape (100 symbols @ 2,000 prices/s)

Same columns as the AWS status. Local has no KPUs or partition billing, so those
read $0 / n/a — kept in place so the two tables line up.

| Condition | in trades/s | in prices/s | out positions/s | out MV/s | Parallelism | Utilization % | Total $/hr | Flink KPU | Flink $/hr | BackPressure | Kafka partitions | Kafka $/hr |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **SQL, defaults (recommended)** | 268 | 2,680 | **341** | **707** | 4 | ~35% | 0.00 | n/a (4 slots) | 0.00 | 0.0% | 4 | 0.00 |
| SQL, conflation **off** | 268 | 2,680 | 341 | ~2,931 | 4 | ~35% | 0.00 | n/a (4 slots) | 0.00 | 0.0% | 4 | 0.00 |
| SQL, input conflation 250ms | 268 | 2,680 | 341 | 374 | 4 | ~35% | 0.00 | n/a (4 slots) | 0.00 | 0.0% | 4 | 0.00 |

*out positions/s = both position topics combined (214 + 127). out MV/s = both
market-value topics combined (577 + 130 on the defaults).*

The input columns are the generator's rate; the pipeline kept up in every run
(0% backpressure), so no ceiling was found locally. The output columns are what
Kafka charges for.

**`rec/s` here is the GENERATOR rate, not capacity.** The pipeline kept up in
every condition — backpressure 0.0%, busy ~35% — so these runs never found a
ceiling. The AWS figures are drain rates against a saturating backlog and measure
something different. **Do not put the two in one table.**

Utilization is measured from the JobManager REST backpressure endpoint
(`scripts/local_utilization.py`), which reports busyRatio / backpressuredRatio
per subtask — the local equivalent of MSF's busyTime / backPressuredTime.

### What actually differs between those three conditions

| Condition | Records published | Staleness p50 | Staleness max | Exact | Ordering violations | Result |
|---|---|---|---|---|---|---|
| conflation **off** | **439,623** | **0ms** | 0ms | **100%** | 0 | **PASS** |
| conflation 250ms | 56,073 | 1,968ms | 2,742ms | 0% | 0 | FAIL |
| conflation 100ms | 57,633 | 6,295ms | 8,275ms | 0% | 0 | FAIL |

400,000 prices / 40,000 trades per run. TaskManager peaked at ~4.7% CPU and
1.9 GB of 7.6 GB — the laptop was never the constraint.

**The 100ms row is the important one:** the same record count as 250ms for
**3x the staleness**. A smaller window attempts more window firings, cannot keep
up, and the shortfall becomes lag. There is no case for tuning the window down.

## Sparse — 10 symbols @ 20 prices/s, P=2

| Condition | Staleness p50 | Staleness max | Exact | Result |
|---|---|---|---|---|
| conflation off | **0ms** | 0ms | **100%** | PASS |
| conflation 250ms, emit 0 | 1,563ms | 3,093ms | 0% | FAIL |
| conflation 250ms, emit 1000ms | 1,564ms | 3,095ms | 0% | FAIL |
| conflation 250ms, idle timeout 500ms | 2,542ms | 4,112ms | 0% | FAIL |
| conflation 100ms | 1,525ms | 4,603ms | 0% | FAIL |
| conflation 50ms | 1,525ms | 4,603ms | 0% | FAIL |

The emission interval is irrelevant (rows 2 and 3 are identical to the
millisecond). Shortening the idle timeout makes it WORSE. At sparse rates
staleness is set by when the next tick arrives, not by window length — which is
why 100ms and 50ms are byte-identical.

## Quiet symbols — do thinly-traded names freeze?

10 symbols tick for 120s, then only 3 continue; symbols 3-9 sit silent for two
full minutes.

| Condition | Exact | Staleness p90 | Staleness max | Result |
|---|---|---|---|---|
| conflation off | **100%** | 0ms | 0ms | PASS |
| conflation 250ms | 50% | 3,625ms | 4,340ms | FAIL |

**They do not freeze.** If idle symbols held their windows open, staleness would
be ~120,000ms. It caps at 4,340ms — the same order as the end-of-stream case, so
a quiet symbol catches up within seconds and stays correct.

## Ordering — every run, every configuration

| Check | Result |
|---|---|
| `as_of` going backwards | **0** |
| out-of-order price used for output | **0** |
| keys ending on a stale value | **0** |

Across ~550,000 published records. A key always hashes to one partition, so
offset order is per-key order — this is what a consumer actually sees. No
position or price ever arrives out of order, and no consumer is left holding a
superseded value.

*(One apparent exception was a harness artifact: restarting the generator resets
its per-symbol tick counter, so prices for still-active symbols drop back to
base. It appeared identically with conflation on and off, so the pipeline
reordered nothing.)*

---

## Bottom line

**SQL is correct.** The single lever is conflation, and it is binary:

| | prices | write volume |
|---|---|---|
| conflation off | exact | ~8x more |
| conflation 250ms | ~2s stale | ~8x less |

Window size is not a useful dial — smaller is strictly worse. Since Kafka data
transfer is the largest cost line on AWS, that 8x is the real price of exact
values.

---

## THE FIX — conflate the output, not the input

Dense run, 100 symbols @ 2,000 prices/s, identical input (440,000 records) in
every condition. `records` = rows PUBLISHED to mv-by-ticker.

| config | output records | staleness p50 | exact | result |
|---|---|---|---|---|
| input conflation 250ms *(what we run today)* | 55,389 | 2,929ms | 0% | FAIL |
| **output conflation 1000ms** | **18,386** | **0ms** | **100%** | **PASS** |
| **output conflation 250ms** | 38,773 | **0ms** | **100%** | **PASS** |
| unconflated | 439,623 | 0ms | 100% | PASS |

**Both output-conflation settings beat the current config on BOTH axes** --
fewer writes AND exact prices. This is not a trade-off; the current setting is
simply dominated.

**Why.** Rate-limiting the OUTPUT publishes the newest value less often, so the
final value is always current. Windowing the INPUT discards the newest tick
before it is ever used, so the final value can never be current. Same volume
goal, opposite correctness outcome. It is exactly what the DataStream job does
with processing-time timers -- which is why DataStream never showed staleness.

**Recommended:** `sql.price.conflate.ms=0` with `mv.emit.interval.ms=1000`.
3x fewer writes than today, and exact. Lower the interval toward 250ms if
consumers need sub-second updates -- a pure freshness/volume dial with no
correctness cost.

Ordering stayed clean in every condition: as_of-backwards=0, price-backwards=0,
keys-ending-stale=0.
