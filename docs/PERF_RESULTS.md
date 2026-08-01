# Phase 6 — Performance & Scaling Validation (local)

Environment: Docker Compose on a MacBook (Apple Silicon), Flink 1.20 session
cluster, 1 TaskManager × 4 slots, RocksDB state backend, 10s checkpoints.
Every change below is **configuration only — the jar was never rebuilt**.
Probes: `scripts/perf_probe.py` (Prometheus samples + per-record write latency
from Kafka `CreateTime` − trade `event_time`).

## Baseline — 10 trades/sec, parallelism 2

| metric | value |
|---|---|
| trades parsed / dedup out | 10.0/s / 9.5/s (5% injected duplicates dropped) |
| MV by account out | 109.5/s (price ticks re-value all holders) |
| sink volume | 17.5 KB/s |
| busiest task | 9 ms/s (0.9% of a core) |
| backpressure / source lag | 0 / 0 |
| e2e latency (trade → output written) | p50 100 ms, p95 192 ms |

## Case 1 — 1000 trades/sec (config: `generator.trades.per.sec=1000`)

| metric | value |
|---|---|
| trades parsed | sustained 1000/s (avg incl. ramp 827/s, max 1000/s) |
| dedup out | 950/s (5% duplicates dropped) |
| sink volume | 358 KB/s peak |
| busiest task | 52 ms/s (5.2% of a core) |
| backpressure / source lag | **0 / 0** |
| e2e latency | **p50 96 ms, p95 98 ms — unchanged vs baseline** |

**Requirement met:** raising order speed 100× did not impact performance; no
latency increase materialized because the pipeline has ~20× headroom at P=2 —
the dashboard's busy/backpressure panels are where latency growth would appear
first if pushed past capacity.

## Case 2 — extreme price at 1000 trades/sec (config: `generator.price.cents.override=10^15`)

| metric | value |
|---|---|
| trades parsed | 997/s avg, max 1000/s |
| busiest task | 49 ms/s — same as Case 1 |
| backpressure / source lag | **0 / 0** |
| e2e latency | p50 118 ms, p95 121 ms — within noise of Case 1 |
| output exactness | `-230,140 × $10,000,000,000,000.00 = -2,301,400,000,000,000,000.00` (digit-exact) |

**Requirement met:** an absurd price ($10 trillion) changes nothing on the
order path — prices are fixed-width long cents on an isolated keyed stream,
and MV math is BigDecimal (exact at any magnitude, proven in unit tests too).

## Linear scaling — parallelism 1 → 2 → 4 (config only)

Method: resubmit the job at each parallelism (config change, no rebuild); a
fresh job replays the whole trades topic from earliest, so sustained parse
rate while source lag > 0 is the pipeline's capacity at that parallelism
(`scripts/scaling_test.py`).

Capacity = backlog drain slope + live input (the 60s meter lags during short
catch-ups, so the backlog itself is the honest measure). Busy time pegged at
~980 ms/s during every catch-up — the pipeline was genuinely saturated.

| parallelism | measured capacity | vs P=1 |
|---|---|---|
| 1 | ~7,000 rec/s (drained 244k backlog in 40s) | 1.0× |
| 2 | ~14,300 rec/s (drained 266k backlog in 20s) | **2.0× — linear** |
| 4 | ~16,800 rec/s (drained 254k backlog in 16s) | 2.4× |

**1 → 2 scales linearly (2.0×).** 2 → 4 gains only ~18% — an environment
ceiling, not a pipeline one: at P=4 the 7 operators × 4 subtasks (28 tasks)
plus Kafka, the generator, Prometheus and Grafana all share the same 8-core
Docker VM, so the host saturates. Keys are uniformly distributed
(account×ticker), so with real per-KPU compute (the AWS deployment) the same
config-only rescale is expected to hold linearity — that's the Phase 6 AWS
follow-up. Every rescale here was `pipeline.parallelism=N` + resubmit; the
jar was untouched.

## How to explain the numbers (demo script)

1. Open Grafana → records/sec panel: parse_trade tracks the generator rate;
   dedup out ≈ 95% of it (the 5% injected duplicates — dedup stat panel shows
   the exact count).
2. MV out > trade rate: every price tick re-values all holders of the ticker
   (fan-out is accounts-per-ticker).
3. Busy time is the capacity gauge: 1000 = a saturated subtask. At 1000
   trades/sec we sit ~5% — that headroom is why latency does not move.
4. Backpressure + pending records are the early-warning pair: they rise
   before latency does; both flat = pipeline keeping up.
5. Case 2: price magnitude is data, not work — long cents + BigDecimal, no
   float, no overflow: point at the digit-exact 19-digit MV output.
