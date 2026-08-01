# I built a production-grade Flink trading pipeline in one day, pairing with AI. Here's every prompt.

*Draft — LinkedIn article*

---

Last Friday I sat down with a 25-line requirements file and Claude Code, and by
the end of the day I had a deterministic Apache Flink trading pipeline: real-time
positions and market values from Kafka, deduplication, a Grafana dashboard,
14 passing tests, a validation suite that re-derives every output independently,
Terraform for AWS, and load-test results showing 1000 orders/sec with flat
latency and measured linear scaling.

Six prompts did the engineering: one asking for a reviewable plan, and five
saying "proceed with phase N." Everything else I typed that day was
housekeeping — logging, git ceremony, a remote check. And because my very
first (housekeeping) prompt was "keep track of all prompts for this session so
we can do a good linkedin article latter" (typo preserved — they all are),
every prompt is recorded in the repo under `prompts/`, phase by phase.

Here's what I learned about working this way.

## 1. Ask for a plan you can review, not code

My second prompt: *"read the requirements and create a plan that we can review
each outcome, the solutin should follow industry best practices."*

What came back wasn't code — it was a design doc (PLAN.md): architecture
diagram, keying strategy, dedup approach, a table of design decisions with
rationale, and **six phases that each end in something a team can review**:

1. Design doc → 2. Walking skeleton on the laptop → 3. Full calculations +
dashboard → 4. Correctness suite → 5. AWS via Terraform → 6. Performance
validation

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

## 6. What I'd tell you about AI pair-building

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

*Built with Claude Code (Fable 5). Total session: one day, 6 working prompts,
6 phases, 6 bugs found and fixed, 0 floats harmed in the making of this
pipeline.*
