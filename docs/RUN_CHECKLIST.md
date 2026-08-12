# Run checklist

Follow this for EVERY measurement run. It exists because the agreed columns kept
getting dropped and broken metrics kept getting reported as data.

---

## Before the run

- [ ] **Code change tested locally first** — `make up`, never straight to cloud
- [ ] **Both engines** — SQL *and* DataStream. Three conclusions in this project
      flipped once the second engine was measured
- [ ] **Metric calibrated against a known truth** — e.g. generator emits a known
      trades/s; if the collector cannot reproduce it, the collector is wrong
- [ ] **Interpretation thresholds declared** before running, not after
- [ ] **Teardown + watchdog armed** for anything cloud

## Status columns — ALL of them, every run

| Condition | in trades/s | in prices/s | out positions/s | out MV/s | Parallelism | Utilization % | Total $/hr | Flink KPU | Flink $/hr | BackPressure | Kafka partitions | Kafka $/hr |

Rules:
- Engine AND platform in Condition: `SQL (AWS)`, `DataStream (local)`, `SQL (Confluent)`
- DataStream rows first, then SQL
- Parallelism is its own column; Utilization immediately after it
- **BackPressure** spelled out, never "bp"
- Local fills cost columns `0.00` and KPU `n/a (N slots)`; Confluent uses CFU

## Correctness columns — every run

| Exact % | Staleness p50/p90/p99/max | as_of backwards | price backwards | keys ending stale | Six-check result |

## Validity gates — a run is INCOMPLETE without these

- [ ] **Rates are counter deltas**, not instantaneous per-second metrics
- [ ] **Job still consuming at sample end** — otherwise the "rate" is
      total ÷ window (this produced identical fake numbers across four runs)
- [ ] **Backpressure from `backPressuredTimeMsPerSecond`**, NOT the
      `/backpressure` endpoint ratio — the endpoint read 0.0% on a job that was
      99.9% blocked
- [ ] **Utilization is a real number**, not "no data" — retry before accepting
- [ ] **Per-subtask, not averaged** — an average of 31% hid sources at 0.2% and
      the bottleneck at 85-93%
- [ ] Backlog sized so it CANNOT drain inside the sample window

## After the run

- [ ] Record to the ledger/evidence file BEFORE starting the next run
- [ ] Commit it
- [ ] State what FAILED as prominently as what passed
- [ ] Tear down; verify at zero
