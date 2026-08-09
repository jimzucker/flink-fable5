# Metric audit — which published numbers survive the per-subtask bug

**The bug.** AWS MSF publishes `numRecordsOutPerSecond` once per **subtask**
under the same `{Application,Task}` dimensions. Read with CloudWatch stat
`Average`, it returns a **per-subtask** rate, not a total.

    total = per-subtask average x subtask count

**Why it matters selectively.** The error is a division by subtask count. So:

* **Comparisons at the SAME parallelism are UNAFFECTED** — the divisor is
  identical on both sides and cancels exactly.
* **Comparisons ACROSS parallelism are INVERTED** — the divisor is the variable
  under test. Under perfect linear scaling the number stays *flat*; under real
  scaling it *falls*.

That is why the damage is concentrated in scaling claims and leaves the
head-to-head throughput ratios standing.

---

## CONTAMINATED — do not publish without re-measurement

| Claim | Where | Why |
|---|---|---|
| "AWS SQL +17% for 2x parallelism (1.17x)" | PHASE19_RESULTS.md:91 | P=20 vs P=40 on the per-subtask metric |
| "Neither platform scaled" | PHASE19_RESULTS.md:84,105 | rests on the above |
| "SQL doesn't convert parallelism into throughput" | PHASE19_RESULTS.md:114 | rests on the above |
| "+83% compute for +17% throughput" | PHASE19_RESULTS.md:97 | throughput half of the ratio is contaminated |
| The scaling narrative | LINKEDIN_ARTICLE.md:93-97,109,141,151-156,176-177 | inherited from PHASE19 |

Phase 20 re-measured the same comparison correctly and got **1.74x**, not 1.17x.
The threshold Phase 19 declared in advance (>=1.6x = genuine scaling) would have
been *met*. The pre-declared threshold was sound; the measurement fed into it
was not.

There is a tell in the Phase 19 write-up itself, at line 40: *"The
application-level metric is not an aggregate rate — during a drain clearing
37.85M records it read ~2,500/s. Absolute values are uninterpretable."* The
anomaly was noticed and explained away as a quirk of the metric rather than
chased. It was the bug, visible at the time.

---

## SURVIVES — same parallelism, or not from this metric at all

| Claim | Where | Why it holds |
|---|---|---|
| **DataStream sustains 5.5x SQL** | PHASE19_RESULTS.md:31-33 | both sides at the SAME parallelism — divisor cancels |
| **Gap widens with cardinality (~2x at 30 symbols -> ~5.5x at 3,000)** | PHASE19_RESULTS.md:35 | same-parallelism comparisons throughout |
| **Confluent SQL 20,564 rec/s** | PHASE19_RESULTS.md:44 | direct count/time (13.86M / 674s), never touched CloudWatch |
| **Confluent drew max 10 CFU at pool caps of 10 and 20** | PHASE19_RESULTS.md:86 | measured as CFU draw, a different metric |
| **Materializer cost** | PERF_RESULTS.md:46-49 | same parallelism both sides |
| **Phase 16 utilisation / KPU cost** | PHASE16_RESULTS.md:102,189 | slot and vCPU counts, not a rate metric |

The headline **DataStream vs SQL throughput advantage is intact.** Only the
*scaling* claim is affected. Those are separate findings and only one of them
broke.

---

## NEEDS RESOLUTION — a genuine conflict, not a metric artifact

`PERF_RESULTS.md:49` and Phase 20 disagree on what the materializer costs
(+14% vs ~+89%). Moot in practice: removing it produces wrong market values,
so neither figure describes a usable configuration. Do not quote either.

Note `PERF_RESULTS.md` throughput numbers came from the local Docker rig via
Prometheus, not CloudWatch, so they are unaffected by the per-subtask bug --
but they have never been cross-checked against a ground truth.

---

## Rule going forward

Any records-per-second figure from MSF must be produced by
`scripts/phase20_totals.py`, which derives the subtask multiplier from
`SampleCount` and prints a calibration check against the generator's known
trades rate. `scripts/aws_perf_probe.py` still contains the older
`TaskOperator`-dimension SEARCH, which matches nothing and returns empty — it
should not be used for operator-level series until fixed.
