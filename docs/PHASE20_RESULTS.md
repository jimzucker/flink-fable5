# Phase 20 — results

**Objective:** find out why Flink SQL doesn't scale.

**Outcome: the premise was wrong.** SQL does scale. The Phase 19 finding it was
built on came from a broken measurement and is retracted. The phase became: fix
the measurement, then establish what is actually true.

Live results land in [PHASE20_LEDGER.md](PHASE20_LEDGER.md) as each condition
finishes. Retractions and their scope: [METRIC_AUDIT.md](METRIC_AUDIT.md).

---

## 1. The measurement was wrong

AWS MSF publishes `numRecordsOutPerSecond` once per **subtask**. Read with
CloudWatch stat `Average` it returns a per-subtask rate, not a total — so every
cross-parallelism comparison was divided by the variable under test. Under
perfect linear scaling that metric stays flat; under real scaling it falls.

| corrected | |
|---|---|
| SQL P=20 → P=40 | **1.74×** (was reported as 1.17×, then 0.98×) |
| per-record cost | **fell ~22%** (was reported as +29%) |

Caught by calibrating against a known truth: the generator emits exactly 400
trades/s, and the metric only reproduced that when multiplied by subtask count.

**Retracted:** "SQL doesn't scale", "neither platform scaled", "+83% compute for
+17% throughput", and the article narrative built on them. Banners are in the
affected documents.

**Survives:** DataStream sustains 5.5× SQL, the gap widening with cardinality,
Confluent's 20,564 rec/s, and the 10-CFU ceiling — all same-parallelism or from
a different metric.

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

## 6. Pending

* Four fixed-backlog scaling runs (SQL and DataStream, P=20 vs P=40) → ledger
* One control run: SQL correctness with the current configuration

## What carries forward

* Calibrate a metric against a known truth before trusting it.
* A test that cannot fail is not evidence — check that the tolerance and the
  signal can both move.
* Profile per component; whole-system averages hide which stage is the limit,
  and a conclusion from one engine does not transfer to another.
* Cost is dominated by Kafka data transfer, not compute.
