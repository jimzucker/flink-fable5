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

## Phase 7 — Price-tick fan-out bottleneck: proven, then fixed by conflation

**Theory (raised in review):** MV joins do O(holders) work per price tick, and
MV backpressure propagates upstream through the shared position operator into
the order path. Proven config-only at 50 accounts, trades constant at 1000/s:

| price rate | MV emissions | busiest task | backpressure | order latency p50 |
|---|---|---|---|---|
| 20/s (baseline) | 1.9k/s | 4% | 0 | 54 ms |
| 200/s (10×) | 11k/s | 9% | 0 | 42 ms |
| 2,000/s (100×) | 101k/s | 33% | 0 | 73 ms (creeping) |
| **10,000/s (500×)** | **517k/s** | **947 ms/s — saturated** | **526 ms/s sustained** | **91 ms, lag growing → unbounded** |

**Fix: conflated re-valuation** (`mv.reval.interval.ms`, default 250 ms; 0 =
per-tick). Position-driven MV stays immediate; price-driven re-valuation runs
on a per-ticker processing-time timer at the latest price, absorbing
intermediate ticks (`demoTicksConflated` metric). Same 500× storm after:

| | per-tick | conflated 250 ms |
|---|---|---|
| prices absorbed | 9,131/s (falling behind) | **10,000/s flat** |
| MV emissions | 452,928/s | 1,883/s (**240× less work**) |
| busiest task | 947 ms/s | 404 ms/s |
| backpressure / lag | 526 ms/s / growing | **0 / 0** |
| order latency p50/p95 | 91/108 ms degrading | **95/97 ms stable** |

99.6% of ticks conflated (>1.4M absorbed in minutes). Semantics unchanged:
the full validation suite passed on the storm data (181k trades, 1.81M prices,
500 position keys — dedup, reproducibility, completeness, exact MV all green),
because final state is still position × latest price. Bound by construction:
price-driven work ≤ holders × (1000/interval) per ticker/sec at any tick rate.

## Phase 8 — AWS results (MSK Serverless + Managed Flink, us-east-1)

Same jar, same config knobs as tfvars; measured with
`scripts/aws_perf_probe.py` (CloudWatch, 1-min granularity, rates calibrated
×parallelism after checking against the known 10/s baseline).

| | AWS baseline (10 t/s, 20 p/s) | Case 1 (1000 t/s) | Storm (10k p/s, 50 accts) |
|---|---|---|---|
| operator parallelism | 2 | 2 | 2 |
| KPUs (processing) | 2 (+1 orchestration billed) | 2 (+1) | 2 (+1) |
| trades parsed | 10.0/s | **1000.0/s sustained** | 1002/s sustained |
| prices parsed | 20.0/s | 20.0/s | **10,002/s — full rate** |
| dedup out | 9.6/s | 950.4/s (5% dups) | 953/s |
| MV emissions | ~53/s (conflated) | ~994/s | **1,499/s** (pre-conflation: 517k/s) |
| busiest task | 5 ms/s | **21.5 ms/s (2.2%)** | 178 ms/s (**~17%**) |
| backpressure | 0 | **0** | **0** |

Case 1 verdict: PASS on AWS — 100× the order rate at 2.2% busy on 2 KPUs
(vs 5.2% on the laptop), zero backpressure. Same jar, tfvar-only change.

Storm verdict: PASS on AWS — the exact load that saturated the pre-conflation
design (sustained backpressure, unbounded lag) runs at ~17% busy on 2 KPUs
with zero backpressure; conflation bounds MV work at 345× below the naive
per-tick demand.

Case 2 verdict: PASS on AWS — every price pinned to $10¹³ at full storm rates
(1000 trades/s + 10,000 prices/s): metrics indistinguishable from the normal-
price storm (busy ~13%, backpressure 0, all rates identical). Price magnitude
is data, not work — confirmed on managed infrastructure.

Measurement note: each AWS test step takes ~7–8 min (ECS config roll ~2 min +
CloudWatch 1-min datapoint granularity + 4-min clean window) vs seconds
locally with Prometheus — an observability-cadence difference, not a pipeline
one.

Capacity note: every AWS test above ran at parallelism 2 on 2 processing KPUs
(1 vCPU / 4 GB each, `ParallelismPerKPU=1`, autoscaling off for determinism).
Headroom at storm load was ~5× on the busiest task; the config-only rescale
path (`terraform apply -var flink_parallelism=4`) is proven but was not needed
to pass any case.

Deployment story and gotchas: [AWS_RUNBOOK.md](AWS_RUNBOOK.md#deployment-gotchas-learned-the-hard-way-2026-08-01).

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
