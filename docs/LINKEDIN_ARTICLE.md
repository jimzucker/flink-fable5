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

**The epilogue: I rebuilt it on a second cloud, then spent days learning to
measure it honestly.** One prompt asked whether the whole thing could run on
Confluent Cloud instead. Their managed Flink speaks SQL, not Java, so the
pipeline became 200 lines of SQL instead of 2,000 lines of Java — and the same
validation suite, unchanged, proved it correct to the cent. That took an
afternoon and $4.

Benchmarking the two fairly took far longer, because most of the numbers I
published first were wrong, and each one was wrong in a way I had to be shown.
I compared one system's cruising speed against the other's sprint. I quoted
compute cost and ignored the rest of the bill. I reported SQL as 124× slower
when what I had measured was my own translation — one SQL statement per Java
operator, so every stage handed off through a Kafka topic. Rewritten as a
single job, the identical logic went from 26.7 seconds to 1.8.

**The worst one looked perfect.** A run finished, reported a clean throughput
number, and I wrote it down. The pipeline had been reading 119,000 records a
second and writing *nothing at all* — for an hour. Status: running. No errors.
CPU busy. Every dashboard green. The cause was mine: I had widened the topics
to 48 partitions to remove a bottleneck, but the data only has ten symbols, so
38 partitions sat permanently empty — and this kind of pipeline waits for
*every* partition before it will release a result. It waited forever.

I had checked, on every single test, that records were going *in*. I had never
once checked that records were coming *out*.

That one mistake had quietly corrupted a headline comparison. Fixing it moved
SQL's throughput up 41%.

**What survived, after re-running everything with the output checked:**

Writing it in Java is about twice as fast as writing it in SQL — same hardware,
same load, same guarantees, same logic. That gap is real and it held up.

SQL runs at the same speed on both clouds. When the two SQL numbers landed
within 3% of each other, the honest reading wasn't "one is faster" — it was
"the language is what costs you, not the vendor."

**And the thing I was most confident about was backwards.** I argued that a
particular optimisation would slow the Java version down, and reasoned it
through carefully. I tested it anyway, and it made it 2.66× faster. Same
optimisation, opposite of my prediction. The measurement took twenty minutes;
the argument would have shipped a version a third as fast.

**Then there was the scaling claim I got to be wrong about twice.** I had
published that one platform "doesn't scale." Then I found the ceiling was
something I'd built into my own test rig, corrected it publicly, and published
a scaling number instead. Then I measured what the machine actually *drew* —
and it never used more than half of what it was allowed. There was no scaling
either way. The real limit was that the business problem has ten symbols in it,
and you cannot spread ten things across twenty workers. Neither platform was
ever the constraint.

**And the cheapest lesson in the project: 64% of the cloud bill wasn't the part
I spent weeks tuning.** It was Kafka — the base charge and the per-partition
fee. All that pipeline optimisation was working on the smaller half of the
invoice.

**So: Java where latency or control matter. SQL where a couple of seconds is
fine and speed-to-build wins — provided you write it as one job, not a chain of
statements, which is a 15× mistake hiding in plain sight.** Both produced
identical results, every time.

But the thing that actually decided it wasn't speed at all. A number on a screen
takes 300–500 milliseconds to read, so anything faster is unreadable flicker —
cost with negative value. I capped the outputs accordingly. In Java it was a
configuration change that measured exactly as designed. In SQL on that platform
I could not express it at all: the mechanism isn't offered, and the workaround
starved the outputs to a fraction of the rate I was trying to allow.

Days of arguing about speed. What settled it was whether either one could do
what the product asked.

**Then I found the bug that made most of it meaningless.**

I had been testing with ten stock symbols. Real US equity markets have eight to
eleven thousand. So I widened it to three thousand, on a realistic curve — a
heavy head, a long tail, plus one hot new listing taking thirty percent of the
tape, the IPO day. I validated the distribution, wrote it into the plan,
committed it, and ran it.

Then a correctness check printed `symbols=30`.

One line in my generator read `Math.min(requested, 30)`. Every run I had ever
done — every throughput number, every scaling conclusion, weeks of work — had
quietly been capped at thirty symbols. The "three thousand symbol" dataset never
existed.

**Almost every ceiling I had found turned out to be that clamp.** With ten
symbols, a system that gives one worker per symbol can only ever use ten
workers. I had been measuring my own test data and attributing it to the cloud
vendors.

**And here's the part I didn't expect.** Earlier I had published that one
platform "doesn't scale." Then I decided that was my mistake and corrected it.
Then I corrected the correction. When I finally re-ran it properly — three
thousand symbols, identical data, only the compute budget changed — the answer
was that **my very first conclusion had been right all along.** A single query
won't use more than about ten units of compute no matter how much you give it.
Both of my confident corrections were wrong. You scale that system by running
more queries, not by buying a bigger one.

**Before any of that, though, I had to admit I'd never checked the answers.**
Days of throughput numbers, and I had never once successfully run the test that
verifies the output is actually correct. It had crashed the one time I tried,
and nothing was gated on it, so nobody noticed. Meanwhile I'd shipped four
changes to windowing and partitioning that I'd argued were safe.

So I stopped and built the check properly: recompute every position and every
market value independently from the raw data, and compare. To make failures
legible I set every trade to one share and gave each symbol its own fixed price
— so a position is just a count you can verify by eye.

It failed. Six hundred market values were wrong.

Except they weren't. They were each priced at a slightly *older* tick of the
right symbol — a real number, just not the newest one. That's a known trade-off
in how these windows close, and I could have waved it away. Instead I made the
checker prove it: does the implied price fall inside the range that symbol
actually traded at? All six hundred did. Zero wrong answers, and a six-percent
staleness characteristic I can now describe honestly instead of hoping nobody
asks.

**The lesson I'd keep isn't about either product. Benchmarks mostly measure the
person running them.** Mine only became trustworthy when I stopped asking "how
fast is it" and started asking "was every part of this actually doing work, and
did anything come out the other end?" Three published conclusions did not
survive that question.

**github.com/jimzucker/flink-fable5**

*One day to build — 5.7 active hours, 8 engineering prompts out of 83 messages.
Then several more days learning to measure it: two clouds, a dozen full re-runs,
and five published conclusions withdrawn — one of which turned out to have been
right the first time. ~$57 of AWS; Confluent covered by trial credit. Every
number in this piece is traceable to a measurement in the repo, including the
ones that replaced earlier numbers.*
