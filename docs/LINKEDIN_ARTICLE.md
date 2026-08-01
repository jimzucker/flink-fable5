# I built a production-grade Flink trading pipeline in one day, pairing with AI. Here's every prompt.

*Draft — LinkedIn article*

---

Last Friday I sat down with a 25-line requirements file and Claude Code, and by
the end of the day I had a deterministic Apache Flink trading pipeline: real-time
positions and market values from Kafka, deduplication, a Grafana dashboard,
18 passing tests, a validation suite that re-derives every output independently,
Terraform for AWS, and load-test results showing 1000 orders/sec with flat
latency and measured linear scaling.

Eight prompts did the engineering: one asking for a reviewable plan, five
saying "proceed with phase N", one review comment that caught a real design
flaw the clean architecture was hiding (more on that below; it's the best
part) — and one that took the whole thing to AWS and measured the scaling
ladder. Everything else I typed that day was housekeeping — logging, git
ceremony, a remote check. And because my very first (housekeeping) prompt was
"keep track of all prompts for this session so we can do a good linkedin
article latter" (typo preserved — they all are), every prompt is recorded in
the repo under `prompts/`, phase by phase.

Here's what I learned about working this way.

## 1. Ask for a plan you can review, not code

My second prompt: *"read the requirements and create a plan that we can review
each outcome, the solutin should follow industry best practices."*

What came back wasn't code — it was a design doc (PLAN.md): architecture
diagram, keying strategy, dedup approach, a table of design decisions with
rationale, and **six phases that each end in something a team can review**:

1. Design doc → 2. Walking skeleton on the laptop → 3. Full calculations +
dashboard → 4. Correctness suite → 5. AWS via Terraform → 6. Performance
validation — and a 7th phase nobody planned: a review finding (section 6)
that got the same treatment: plan, prove, fix, verify.

That structure did more for quality than any individual piece of code. Every
"proceed with phase N" prompt had a defined finish line, and nothing moved
forward unverified.

## 2. The demo is a real system, not a toy

- **Inputs:** Kafka topics of block trades (trade_id/account/ticker/qty) and
  ticking prices (symbol/price), from a seeded, rate-configurable generator
- **Outputs:** position by account+ticker, position by ticker, market value by
  both — as keyed upsert streams
- **Best practices baked in:** dedup via keyed state with TTL, money as long
  cents + BigDecimal (no floats, ever), every operator instrumented (records,
  rates, KB/sec), RocksDB + incremental checkpoints, per-operator metrics into
  Prometheus/Grafana locally and CloudWatch on AWS
- **Everything is configuration:** rates, parallelism, dedup TTL, even "pin
  every price to $10 trillion" — no rebuild for any behavior change

## 3. Make the system prove itself

Phase 4 was my favorite. Beyond 14 unit tests against the *real* operators
(including a hand-computed golden dataset), we wrote a validation script that
pauses the generator, dumps all six Kafka topics, and **independently recomputes
every output in Python** — dedup counts, both position rollups, the completeness
invariant (sum of account positions == ticker position), and every market value
to the exact penny.

On live data: 20,210 trades, 959 duplicates correctly dropped, 50 position
keys, all exact. When someone asks "how do you know the numbers are right,"
the answer is a script, not a shrug.

## 4. The performance results

All config-only changes, measured with Prometheus + per-record write latency:

| test | result |
|---|---|
| Baseline (10 trades/s) | 0.9% busy, p50 latency 100 ms |
| **Case 1: 1000 trades/s** | sustained, 5% busy, zero backpressure, **p50 96 ms — unchanged** |
| **Case 2: $10¹³ price @ 1000/s** | identical perf; MV digit-exact to 19 digits |
| Scaling P=1→2 | 7,000 → 14,300 rec/s — **2.0×, linear** |
| Scaling P=2→4 | host-limited (28 subtasks on an 8-core Docker VM) — AWS is the fair test |
| Price storm 10,000 ticks/s (post-fix) | order latency flat, 99.6% of ticks conflated, **240× less work** |
| AWS (same jar, MSK + Managed Flink) | every case re-passed; storm at 17% busy on 2 KPUs |
| AWS scaling ladder P=2→4→8 | **16.5k → 52k → 97k msgs/s** — config-only rescales, zero restarts |
| Finale: P=12 vs the 110k msgs/s mega-load | **sustained 1:1 with the sources, ~45% headroom** — as the capacity model predicted |

## 5. The bugs are the best part

Six real issues surfaced, and each one taught something:

1. **Restarted generators produced zero output** — same seed meant same trade
   IDs, and dedup *correctly* ate the entire replay as duplicates. The system
   was right and the test was wrong. Fix: run-id-namespaced trade IDs.
2. **Prometheus silently sanitizes metric labels** (`parse-trade` →
   `parse_trade`) — dashboard queries matched nothing until checked against
   actual label values.
3. **`kafka-console-consumer --timeout-ms` never fires on a busy stream** —
   it's a quiet-stream timeout. Latency is now measured from Kafka record
   CreateTime instead.
4. **Grafana's default color palette failed colorblind-safety validation**
   (orange↔green ΔE 6.2) — replaced with a validated palette, colors pinned
   per operator.
5. **Jackson core/databind version clash** from a transitive dependency —
   NoSuchMethodError at runtime, pinned explicitly.
6. **Docker's stale single-file bind-mount** — Maven replaced the jar, the
   container kept serving the old inode. Mount the directory, not the file.

None of these were in the happy path. All of them would have bitten in
production. The phase-gated "demo it before moving on" discipline is what
surfaced them on day one instead.

## 6. The best catch was human

After six phases, the pipeline was "windowless by design" — every output a
continuous per-event aggregation, and I'd been told that's what made it
deterministic and fast. Then I asked the question any market-data person
would ask: *prices can tick extremely fast — if you 10× the price rate,
won't re-valuing every holder on every tick bottleneck?*

It did. The AI turned my hunch into a config-only experiment: at 10,000
ticks/sec the market-value join was emitting **517,000 records/sec**,
saturated, with backpressure propagating through a shared operator into the
order path — quietly breaking the very guarantee we'd demonstrated two
phases earlier. The demo defaults (5 accounts, 20 ticks/sec) had hidden it
completely.

The fix was the one *deliberate* window in the system: conflated
re-valuation. Position updates still emit instantly; price-driven
re-valuation fires at most every 250 ms at the latest price. Same storm
after: **240× less work, zero backpressure, flat order latency, 99.6% of
ticks absorbed** — and the entire validation suite still passed, because
final state is still position × latest price.

That's the division of labor in one anecdote: the domain instinct came from
the human; the proof, the fix, and the regression suite came from the AI —
in about an hour, as Phase 7.

## 7. Then we turned the real dial

The same day, the same jar went to AWS — MSK Serverless + Managed Service
for Apache Flink, all Terraform. The deployment itself found five more
gotchas (Java 11 runtimes, arm64 vs amd64 images, a stale-build trap, an
AWS tag race, and the fact that Managed Flink silently drops custom metrics
unless they're in a special metric group — all now in the repo's runbook).
Then the question that matters: *can it handle the volume?* We offered the
pipeline more than it could chew and turned one Terraform variable:
parallelism 2 → 4 → 8 processed 16.5k → 52k → 97k messages/sec at
saturation, with zero job restarts across rescales — and the finale run at
P=12 sustained the full 110k msgs/sec mega-load 1:1 with the sources,
exactly where the capacity model said it would.

One picture carries the whole argument — per-subtask throughput across the
session: the overload rungs, the quiet drain, then the flat plateau of P=12
holding 110k messages/sec steady (in the repo:
`docs/images/aws-ladder-throughput.png` — upload it with this article). Volume is a dial, the
dial costs ~$0.12/hour per 12k msgs/sec, and the next 3-5× (binary
serialization instead of JSON) is identified and priced before anyone
needs it.

## 8. What I'd tell you about AI pair-building

- **Gate on outcomes, not output.** "Create a plan we can review each outcome"
  was the highest-leverage sentence of the day.
- **Make verification part of the deliverable.** Golden tests with
  hand-computed numbers, independent recomputation, saturation-measured
  scaling — ask for the proof, not just the feature.
- **Keep the receipts.** Prompts and responses live in the repo next to the
  code they produced. The git history (one squash commit per phase, tagged
  Phase-2 through Phase-6) reads like a build log.
- **The AI catches its own mistakes if you make it look.** Every phase ended
  with the system running and measured — that loop, not the code generation,
  is where the quality came from.

The repo — requirements, plan, code, tests, Terraform, perf results, and every
prompt — is here: **https://github.com/jimzucker/flink-fable5**.

*Built with Claude Code (Fable 5). Total session: one day, 8 working prompts,
8 phases, 6 bugs + 1 design flaw + 8 deployment gotchas found and fixed,
0 floats harmed in the making of this pipeline.*
