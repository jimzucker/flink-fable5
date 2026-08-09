# Phase 20 — results

**Objective:** find out why Flink SQL doesn't scale.

**Outcome: the premise was wrong.** SQL does scale. The Phase 19 finding it was
built on came from a broken measurement and is retracted. The phase became: fix
the measurement, then establish what is actually true.

Live results land in [PHASE20_LEDGER.md](PHASE20_LEDGER.md) as each condition
finishes. Retractions and their scope: [METRIC_AUDIT.md](METRIC_AUDIT.md).

---

## 0. Headline — both APIs scale, DataStream scales better

Same feed, same fixed-backlog method, same cost per rung.

| Condition | rec/s | Parallelism | Utilization % | Total $/hr | Flink KPU | Flink $/hr | BackPressure | Kafka partitions | Kafka $/hr |
|---|---|---|---|---|---|---|---|---|---|
| **DataStream (AWS)** | 12,487 | 20 | 35.3% | 3.67 | 6 | 0.66 | 4.0% | 48 | 3.01 |
| **DataStream (AWS)** | 28,357 | 40 | 17.4% | 4.22 | 11 | 1.21 | 4.3% | 48 | 3.01 |
| **SQL (AWS)** | 6,106 | 20 | 44.3% | 3.67 | 6 | 0.66 | 14.9% | 48 | 3.01 |
| **SQL (AWS)** | 10,363 | 40 | 31.1% | 4.22 | 11 | 1.21 | 1.7% | 48 | 3.01 |

Effective slots working: ~7 of 20 and ~7 of 40 (DataStream), ~9 of 20 and ~12 of
40 (SQL).

**Scaling: DataStream 2.27x, SQL 1.70x.** The gap widens with parallelism —
DataStream is 2.05x faster at P=20 and **2.74x faster at P=40**. Buying compute
helps a DataStream job more than it helps the equivalent SQL job.

DataStream's 2.27x is superlinear and should be read as approximate: at P=20 it
was only 35% busy, so P=40 relieved a constraint rather than adding usable
capacity. It puts ~7 effective slots to work at BOTH rungs — 2.27x more
throughput from the same working compute. The direction is solid; the exact
multiple is not.

**Every configuration is over-provisioned** against a 60-70% sizing target. The
closest was SQL P=40 at ~12 of 40 slots working.

---

## 1. The measurement was wrong

AWS MSF publishes `numRecordsOutPerSecond` once per **subtask**. Read with
CloudWatch stat `Average` it returns a per-subtask rate, not a total — so every
cross-parallelism comparison was divided by the variable under test. Under
perfect linear scaling that metric stays flat; under real scaling it falls.

| corrected | |
|---|---|
| SQL P=20 → P=40 | **1.70×** final, fixed-backlog (reported as 1.17×, then 0.98×; 1.74× on an interim live-load method) |
| per-record cost | **fell ~22%** (was reported as +29%) |

Caught by calibrating against a known truth: the generator emits exactly 400
trades/s, and the metric only reproduced that when multiplied by subtask count.

**Retracted:** "SQL doesn't scale", "neither platform scaled", "+83% compute for
+17% throughput", and the article narrative built on them. Banners are in the
affected documents.

**Survives:** the gap widening with cardinality,
Confluent's 20,564 rec/s, and the 10-CFU ceiling — all same-parallelism or from
a different metric. (Phase 19's 5.5× DataStream advantage is superseded by the
fixed-backlog measurement above: 2.05× at P=20, 2.74× at P=40.)

## 2. A real correctness bug, found and fixed

Both market-value operators stored a price timestamp and never compared against
it, so the **last price to arrive won, not the newest**. Arrival order is not
event order here: adaptive keying deliberately spreads a hot symbol across
partitions to break the IPO hotspot, and Kafka orders only within a partition.

Measured on a salted feed: **461,523 of 866,000 prices arrived out of order.**
The majority case, not a rare race. Every market value published from the
DataStream path on a salted feed was suspect.

Fixed by rejecting ticks older than the one held, matching the guard
`LocalPriceConflator` already had. Validated: all six checks exact, 945/1000
keys matching precisely.

## 3. The correctness harness could not fail — twice

* **Phase 18** used a *static* price per symbol. Identical values cannot reveal a
  reordering, so the checks passed with no guard in the code at all.
* **The first fix** made prices monotonic but left the tolerance at the symbol's
  whole-run price range, so *any* price the pipeline ever emitted was accepted.

Both configurations passed everything. Neither could have failed. Widening the
signal without bounding the tolerance just moved the vacuum. Permitted staleness
is now bounded to the conflation interval (`validate.lag.ms`, default 2000).

## 4. Why SQL is slower — the bottleneck

Per-operator profiling (the `TaskOperator` dimension does not exist; the real
dimension set is `{Application,Task}`):

```
GroupAggregate_7    busy avg 98.6%   skew 1.0
GroupAggregate_12   busy avg 94.1%   skew 1.1
```

**Uniformly saturated, not skewed.** Every subtask equally loaded — a genuine
capacity limit, which is consistent with SQL scaling when given more.

This rejected the leading hypothesis (our own 30% IPO hot key). Note the
rejection holds **for SQL only**: DataStream shows skew 3.1–4.1 on its
market-value operators, so the hot key *is* its mechanism. Profiling one engine
and generalising was an error.

## 5. Cost

| | documented | actual |
|---|---|---|
| AWS Kafka | $0.75/hr | **$3.01/hr** under load |
| AWS all-in | $2.39/hr | **~$4.22/hr** |
| Confluent | $3.36/hr + eCKU | eCKU never measured |

MSK Serverless was 74% of a $40 day — data transfer ($14.35) and partition-hours
across all six topics, neither of which the TCO counted. **The claim that AWS is
29% cheaper on infra TCO does not survive**, and the missing eCKU figure means
neither direction can be proven.

Separately, MSF at `log_level=INFO` ingested 8.25 GB of CloudWatch Logs in one
day — a single day of the monthly free tier. Now defaults to WARN.

## 6. Open

* **SQL market-value correctness is unresolved.** SQL produces ZERO exact
  matches: ~93% of keys land inside a 2,000ms lag tolerance and a ~3% tail falls
  outside. DataStream on the same feed matched 945/1000 exactly. Removing the
  upsert materializer was investigated and is NOT the cause — the control with it
  enabled failed by a wider margin. A tolerance sweep (2s/5s/10s) would separate
  genuine staleness from an over-strict threshold.
* **Confluent's ~10 CFU ceiling** remains the only true scaling failure in the
  project, and its decisive test — two concurrent statements on one pool — is
  still unrun. That is the next phase.

## What carries forward

* Calibrate a metric against a known truth before trusting it.
* A test that cannot fail is not evidence — check that the tolerance and the
  signal can both move.
* Profile per component; whole-system averages hide which stage is the limit,
  and a conclusion from one engine does not transfer to another.
* Cost is dominated by Kafka data transfer, not compute.
