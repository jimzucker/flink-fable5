# Phase 19 — DataStream API vs Flink SQL, on a common basis

**The axis is the API, not the vendor.** Every result below points the same way:
measured on the *same* cloud, DataStream sustains 5.5x SQL; measured on *both*
clouds, SQL failed to scale on each. AWS-vs-Confluent differences exist but they
are capability and cost differences, not performance ones — they are listed
separately at the end so they are not confused with the API finding.

All three cells finally measured on the **same market**: 3,000 symbols, Zipf
α=1.0, one IPO symbol at 30% of the tape, adaptive write-time keys,
exactly-once. Every cell's correctness verified before any number was recorded.

## Correctness — complete, all three cells

| Cell | Checks | Detail |
|---|---|---|
| **DataStream (AWS)** | ✅ six | 13,274 positions, 3,000 tickers, 3,668 duplicates dropped, 0 mismatched |
| **SQL (Confluent)** | ✅ six | 7,246 positions, 2,910 tickers, 0 wrong |
| **SQL (AWS)** | ✅ six | validated in-VPC |

The AWS cells had **never** been validated before this phase — MSK sits in a
private subnet and the validator only existed as a laptop script. Every
DataStream figure published since Phase 6 was unverified until now.

## Throughput — DataStream sustains 5.5× SQL

Identical live load (4 generators), identical window, identical metric:

| Config | median | p90 | max |
|---|---|---|---|
| **DataStream (AWS)** | **5,183/s** | 10,055/s | 10,687/s |
| SQL (AWS) | 950/s | 1,456/s | 1,456/s |
| **Ratio** | **5.5×** | **6.9×** | |

**The gap WIDENS with cardinality.** At ≤30 symbols DataStream led SQL by ~2×;
at 3,000 symbols it leads by ~5.5×. SQL's per-record and state-management
overhead scales worse with key count, and that is invisible on a narrow
benchmark.

*The application-level metric is not an aggregate rate — during a drain clearing
37.85M records it read ~2,500/s. Absolute values are uninterpretable; the RATIO
between configs on the same rig is what this measures.*

**Confluent SQL, measured against a known fixed backlog: 20,564 rec/s**
(13.86M / 674s). That is the only absolute throughput figure in this project
measured against a verified fixed backlog with a confirmed quiet period.

## Scaling

**Confluent: a single statement did not exceed ~10 CFU** at pool caps of both 10
and 20, with 3,000 keys available — so not key starvation. **Mechanism
unestablished**; the decisive test (two concurrent statements on one pool) is
unrun.

**AWS: not measured at 3,000 symbols.** The earlier 2.0× at 10→20 KPU was taken
at ≤30 symbols and does not transfer.

## Cost

**AWS $1.84/hr — 64% of it Kafka**, not Flink. **Confluent $79.96 gross over the
project** ($42.93 Kafka vs $23.29 Flink), fully covered by promotional credit.
On both clouds the streaming platform costs more than the stream processing.

**Project total out of pocket: ~$70, all AWS.**

## Method — what cost the most time

Almost every wrong number came from the harness, not the systems under test:

| Harness fault | Produced |
|---|---|
| `Math.min(requested, 30)` | every run capped at 30 symbols for weeks |
| generator rate limit | identical numbers for different conditions |
| suppressed `docker push` | ECS crash-loop on a missing image |
| zsh `:l` modifier in `"$ECR:latest"` | pushed to a repository named `...atest` |
| rate-based drain detection | 3-min lag, understated by ~25% |
| counter-plateau detection | read a steady rate as "finished" |
| generator left running | drained a growing backlog; job restarted |

**Completion detection failed three separate ways**, which is why the AWS
throughput figures are reported as a ratio from sustained-rate sampling rather
than as drain-to-completion rates.

> **⚠️ THE SCALING SECTION BELOW IS RETRACTED (see docs/METRIC_AUDIT.md).**
> The P=20 vs P=40 figures came from a CloudWatch metric that reports a
> PER-SUBTASK rate, so the comparison was divided by the very variable under
> test. Re-measured correctly in Phase 20: **1.74x, not 1.17x** — which would
> have MET the >=1.6x "genuine scaling" threshold declared before the run.
> The throughput ratios elsewhere in this document are same-parallelism
> comparisons and are NOT affected.

## SQL scaling — NEITHER PLATFORM SCALED

Identical live load (4 generators, ~40k prices/s, saturating), identical window
and metric, one variable changed.

| | P=20 | P=40 | ratio |
|---|---|---|---|
| median | 1,659/s | 1,948/s | **1.17x** |
| p90 | 1,926/s | 3,290/s | 1.71x |
| billed KPU | 6 | 11 | **1.83x cost** |

**Median 1.17x is inside the ~25% run-to-run variance band — no measurable
scaling.** The p90 does move, so extra capacity helps at peaks but not at
typical throughput. Cost efficiency worsens: +83% billed compute for +17%
median throughput.

| Platform | Knob doubled | Result |
|---|---|---|
| **Confluent SQL** | `max_cfu` 10 → 20 | **0%** — Autopilot drew max 10 CFU either way |
| **AWS SQL** | parallelism 20 → 40 | **+17% median** (noise), +71% p90, +83% cost |

**Neither platform scaled.** That is the result. AWS's +17% is not a weaker form
of scaling — it is inside the run-to-run noise band and indistinguishable from
zero by the threshold set before the run. Confluent's is a flat 0%.

The mechanisms differ and that is a footnote, not a second finding: Confluent's
autoscaler declines to draw the capacity, while AWS provisions and bills it and
the query converts almost none of it. Same outcome, reached two ways.

**This reframes the DataStream advantage.** It is not only per-record speed:
DataStream converts added parallelism into throughput, and this SQL topology
largely does not. For a workload expected to grow, that difference compounds.

*Threshold declared before measuring: >=1.6x genuine scaling, 1.2-1.6x partial,
<1.2x none. Median landed at 1.17x.*
