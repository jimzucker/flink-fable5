# Phase 19 — the four lenses on a common basis

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
