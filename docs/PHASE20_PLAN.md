# Phase 20 — why doesn't Flink SQL scale?

**Scope: diagnosis, not benchmarking.** Phase 19 established *that* SQL does not
convert added compute into throughput on either cloud. It did not establish
*why*, and "we don't know" is where that finding currently sits.

| Measured in Phase 19 | |
|---|---|
| Confluent, `max_cfu` 10 → 20 | **0%** — autoscaler kept drawing 10 CFU |
| AWS, parallelism 20 → 40 | **+17%** (inside ±25% noise) for **+83%** cost |

Every measurement so far has been **whole-pipeline**. That cannot identify which
stage refuses to parallelise. This phase adds per-operator visibility first, then
tests hypotheses one variable at a time.

---

## Hypotheses, most likely first

### H1 — Our own dataset has a 30% hot key (skew ceiling)

The realistic market we built puts **one IPO symbol at 30% of the tape**. Flink
assigns one key to one worker, so a single subtask owns 30% of the work
regardless of parallelism. By Amdahl's law that caps speedup at **~3.3×** — and
adding parallelism past that point buys almost nothing.

**This would look exactly like "SQL doesn't scale."** It would also mean the
finding is mis-stated: the honest version becomes *nothing scales on a feed with
a 30% hot key unless the key is spread*, which is a property of the workload, not
the API.

Counter-evidence to weigh: DataStream ran the **same** skewed feed and sustained
5.5×. If skew alone were the cause, DataStream should suffer equally. So H1 is
likely *a* factor, not the whole story.

### H2 — The SinkUpsertMaterializer

`mv-by-account-ticker` still carries an unresolved upsert key (Confluent names it
`UPSERT_AND_PRIMARY_KEYS_DIFFERENT`; MSF inserts the same operator silently). It
was measured moving 219k records/sec — an extra stateful stage the DataStream job
does not have, and a plausible serialisation point.

### H3 — A genuinely non-parallel stage in the SQL plan

The fused statement set may contain an operator the planner runs at parallelism
1, or a chain that pins parallelism to the narrowest member. `EXPLAIN` and the
job graph will show declared parallelism per operator.

### H4 — State/checkpoint overhead grows with parallelism

More subtasks means more state instances and more checkpoint coordination. Past a
point the overhead could cancel the added capacity. Would show as checkpoint
duration rising with parallelism.

### H5 — Confluent Autopilot scales on a signal a drain doesn't produce

Separate from AWS. Autopilot provisions "for current load"; a backlog drain may
not present the lag/backpressure shape it reacts to. **Decisive test (still
unrun): two concurrent statements on one pool.** If total draw exceeds 10 CFU the
limit is per-statement; if not, it is the pool or the autoscaler.

---

## Step 1 — find the bottleneck operator (no new hypotheses required)

MSF publishes per-operator metrics under the `TaskOperator` dimension. For the
SQL job at P=20 and P=40, collect per operator:

* `backPressuredTimeMsPerSecond` — **the operator immediately upstream of the
  bottleneck backpressures.** This points at the culprit directly.
* `busyTimeMsPerSecond` — which operators are saturated
* **max vs mean across subtasks** — a high max with a low mean *is* skew (H1);
  uniformly high means a genuine capacity limit
* `numRecordsInPerSecond` per operator — where the pipeline narrows

This is diagnosis, not inference. If one operator sits at 100% busy while the
rest idle, that operator is the answer and most hypotheses below become moot.

## Step 2 — controlled experiments, one variable each

| # | Change | Isolates | Predicted if hypothesis true |
|---|---|---|---|
| A | **uniform** distribution, no IPO, no Zipf | H1 skew | SQL scales → skew was the cause |
| B | `sql.sink.upsert.materialize=NONE` | H2 materializer | scaling improves |
| C | salt factor 8 → 64 | H1 via spreading | scaling improves without changing the feed |
| D | DataStream on the **same** skewed feed, P=20 vs P=40 | H1 vs API | if DataStream scales and SQL doesn't on identical skew, skew is not sufficient |
| E | two concurrent Confluent statements, one pool | H5 | total draw >10 CFU → per-statement limit |

**D is the control that matters most.** It separates "the workload can't scale"
from "SQL can't scale it," and we already know DataStream sustains 5.5× on this
feed — but we have never measured DataStream's *scaling* at 3,000 symbols.

## Step 3 — read the plan

`EXPLAIN` the statement set and record declared parallelism per operator, plus
which operators the planner chained. A chain runs at one parallelism; a single
narrow member pins the whole chain.

---

## What would change the published claim

* **A scales** → retract "SQL doesn't scale"; publish "a 30% hot key caps
  speedup at ~3.3× for any engine; spread it at the key."
* **D shows DataStream also fails to scale on this feed** → the current claim is
  wrong and the finding is about the workload, not the API.
* **B scales** → the materializer is the bottleneck; fixable by resolving the
  upsert key, which we already know how to do.
* **Nothing scales in any configuration** → the claim stands and is stronger,
  with a named mechanism.

## Method rules carried forward

* Correctness gates performance, at the volume claimed.
* Declare interpretation thresholds **before** each run.
* ~25% run-to-run variance: a gap under that is not a result.
* Compare rates, not drain times; purge between runs.
* Per-operator max vs mean is the skew signal — a whole-pipeline average hides it.
* When two conditions that should differ return the same number, suspect the
  harness.
