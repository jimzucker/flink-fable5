# I built a production-grade Flink trading pipeline in one day, pairing with AI

*Draft — LinkedIn article (short). Full version with every detail: LINKEDIN_ARTICLE_FULL.md*

---

Last Saturday: a 25-line requirements file, Claude Code, and one day. By evening:
a deterministic Apache Flink pipeline turning Kafka trades and prices into
live positions and market values — 18 tests, an independent validation suite,
Terraform, and load tests passed on both my laptop and AWS.

**Eight prompts did the engineering.** One asked for a reviewable plan. Five
said "proceed with phase N." One was a review comment that caught a real
design flaw. One took it to AWS. Every prompt is recorded in the repo.

**Three things made it work:**

**1. Gate on outcomes, not output.** My first real prompt: *"create a plan
that we can review each outcome."* Every phase ended with the system running
and measured — a skeleton on day one, then calculations + dashboards, then a
correctness suite that re-derives every output independently from the raw
topics. Nothing moved forward unverified. That loop, not the code generation,
is where the quality came from.

**2. The best catch was human.** After six phases the pipeline looked done —
1000 orders/sec with flat latency. Then I asked the question any market-data
person would: *if prices tick 10× faster, won't re-valuing every holder on
every tick bottleneck?* It did — the AI turned my hunch into an experiment
that hit 517,000 records/sec of wasted work and backpressure leaking into
the order path. The fix (conflated re-valuation, 250 ms) cut the work 240×
with zero semantic change — the whole validation suite still passed. Domain
instinct from the human; proof, fix, and regression tests from the AI, in
an hour.

**3. Volume should be a dial — so we measured the dial.** Same jar to AWS
(MSK + Managed Flink, all Terraform). One variable, `flink_parallelism`:
2 → 4 → 8 processed 16.5k → 52k → 97k msgs/sec, zero restarts. The finale:
**P=12 sustained 110,000 msgs/sec 1:1 with the sources** — exactly where the
capacity model predicted, at ~$0.12/hour per 12k msgs/sec. Then
`terraform destroy`, verify the zeros. Total AWS bill: about $5.

**It wasn't flawless — that's the point.** Seventeen things went wrong
during the day: 6 code bugs, 1 design flaw, 8 AWS deployment surprises,
2 cleanup surprises. Every one was caught the same way — each phase had to
run and be measured before we moved on. In production, those same problems
would have been incidents. All of it is in the repo: the plan, the code,
the tests, the results, and all eight prompts.

**github.com/jimzucker/flink-fable5**

*One day — 5.7 active hours. 8 engineering prompts out of 83 messages.
394M tokens of AI (≈$515 metered — flat-rate in practice). $5 of AWS.
300M price updates absorbed by one well-placed window. Every number above
traceable to a measurement in the repo.*
