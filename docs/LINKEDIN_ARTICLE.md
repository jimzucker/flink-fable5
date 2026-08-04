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

**The epilogue: I rebuilt it on a second cloud, then spent three days
learning to measure it honestly.** One prompt asked whether the whole thing
could run on Confluent Cloud instead. Their managed Flink speaks SQL, not
Java, so the pipeline became 200 lines of SQL instead of 2,000 lines of
Java — and the same validation suite, unchanged, proved it correct to the
cent. That part took an afternoon and $4.

Benchmarking the two fairly took far longer, because every early number I
published was wrong in a way I had to be shown. I compared one system's
cruising speed against the other's sprint. I quoted compute cost and
ignored the rest of the bill. I cited a configuration nobody could actually
deploy. And the worst one: I reported SQL as 124× slower on latency, when
what I had really measured was my own translation — I had written one SQL
statement per Java operator, so every stage handed off through a Kafka
topic. Rewritten as a single job, the identical logic went from 26.7 seconds
to **1.8**, and the worst case from 55 seconds to **2.1**.

So the real gap is about seven times, not a hundred — and even that isn't
really about SQL. The Java version writes each result the instant it is
computed; the SQL version holds results and publishes them together at its
next safety checkpoint, a couple of seconds apart. That is a
safety-versus-speed setting, and I had them set differently on each side
without noticing. I never re-ran them matched, so I can say it explains most
of the remaining gap, not that I proved it.

**Then a product requirement settled it better than any benchmark had.**
A number on a screen takes 300–500 milliseconds to read, so anything
updating faster is unreadable shimmer — cost with negative value. I capped
the outputs accordingly: positions at most twice a second, market values
once. On the Java side that was a configuration change, and it measured
exactly as designed: 0.96 updates per key per second against a ceiling of
one, median latency 604 milliseconds, and 53,001 values re-checked to the
cent. On the SQL side I could not express it at all. The idiomatic
mechanism isn't exposed on that platform; the windowing alternative opened
172,800 windows per key and starved the outputs to a fraction of the rate I
was trying to allow; two further routes were rejected outright.

I had spent three days arguing about how fast each one was. What actually
decided it was whether either could do what the product asked.

**So: DataStream where latency or control matter — and it's cheaper too.
SQL where a couple of seconds is fine and speed-to-build wins, provided you
write it as one fused job rather than a chain of statements, which is a 15×
mistake hiding in plain sight.** Both produced identical results, every time.

The lesson I'd keep isn't about either product. **Benchmarks mostly measure
the person running them** — and sometimes they measure the wrong thing
entirely. Mine only became trustworthy because a reviewer kept asking what
else was different, and because a test suite that re-derives truth from raw
data made every re-measurement cheap enough to bother with.

**github.com/jimzucker/flink-fable5**

*One day — 5.7 active hours, 2.6 of them me reviewing and steering.
8 engineering prompts out of 83 messages. 394M tokens of AI (≈$515
metered — flat-rate in practice). $5 of AWS. 300M price updates absorbed
by one well-placed window. Plus a Sunday afternoon and $4 for the
Confluent second opinion. Every number above traceable to a measurement
in the repo.*
