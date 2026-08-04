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

**The Sunday epilogue: same tests, second cloud.** One more prompt:
*could the whole thing run on Confluent Cloud instead?* Their managed
Flink speaks SQL, not Java — so the pipeline was rewritten as six SQL
statements, and the same validation suite, unchanged, proved the rewrite
correct: every position and market value exact to the cent after 300,000
trades. Then it beat Saturday's throughput record — 232,000 messages/sec,
2.1× the AWS finale — at an estimated monthly cost about the same as AWS
at volumes under ~10,000 messages/sec — and sized to the identical
110k msgs/sec job, roughly 40% less. About $4 of Confluent,
torn down by evening. That's the quiet
payoff of tests that re-derive truth from raw data: switching engines, or
clouds, becomes an afternoon, not a quarter.

**The Monday postscript: measuring it properly.** The first comparison had
Confluent ahead — 232,000 messages/sec against AWS's 110,000. It was wrong,
and not in Confluent's favor: I had measured AWS at cruising speed and
Confluent at a sprint. Fixing that took four rounds of my own review
catching my own mistakes — compare systems at the same operating point,
count the whole infrastructure bill and not just compute, quote configs a
person can actually deploy, and give each platform the same tuning
courtesies. Every correction is in the repo.

**Then I caught myself.** My scoreboard said SQL was 124× slower — 33
seconds against 267 milliseconds. A reviewer's question ("is that really
the language, or how you wrote it?") sent me back to measure properly. It
was how I wrote it. I had translated the Java pipeline one operator per SQL
statement, so every stage handed off through a Kafka topic, paying a
checkpoint commit at each hop. Rewriting the identical logic as **one fused
statement set** took the median from 26.7 seconds to **1.8** and the 99th
percentile from 55 seconds to **2.1**. The real gap is about 7×, not 124×.
Most of what's left isn't the language either — it's that one side was
publishing immediately and the other was committing transactionally.

**So the honest scoreboard reads:** DataStream is meaningfully faster and
cheaper; SQL costs a tenth of the code and has nothing to deploy; both
produce identical results to the cent. Sub-second budgets belong on the
first. Anything tolerating a couple of seconds gets built far faster on the
second — provided you write it as one fused job rather than a chain of
statements, which is a 15× mistake hiding in plain sight. The test suite
that proved both versions correct never had to change, which is the only
reason any of this was cheap enough to find out.

**github.com/jimzucker/flink-fable5**

*One day — 5.7 active hours, 2.6 of them me reviewing and steering.
8 engineering prompts out of 83 messages. 394M tokens of AI (≈$515
metered — flat-rate in practice). $5 of AWS. 300M price updates absorbed
by one well-placed window. Plus a Sunday afternoon and $4 for the
Confluent second opinion. Every number above traceable to a measurement
in the repo.*
