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

## Dense — the realistic shape (100 symbols @ 2,000 prices/s, P=4)

| Condition | Records published | Staleness p50 | Staleness max | Exact | Ordering violations | Result |
|---|---|---|---|---|---|---|
| SQL, conflation **off** | **439,623** | **0ms** | 0ms | **100%** | 0 | **PASS** |
| SQL, conflation 250ms | 56,073 | 1,968ms | 2,742ms | 0% | 0 | FAIL |
| SQL, conflation 100ms | 57,633 | 6,295ms | 8,275ms | 0% | 0 | FAIL |

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
