# Phase 23 — 100 symbols, 2 → 4 partitions, live vs backlog

**Local only.** Four runs, one variable pair: partition count and whether the
producer runs concurrently with the consumer.

---

## Pipeline under test

| | |
|---|---|
| Symbols | 100 unique |
| Source partitions | **2**, then **4** |
| Price window | tumbling, **250ms** |
| Outputs | `position/account/symbol`, `position/symbol`, then join prices → `mv/account/symbol`, `mv/symbol` |

Four outputs, two key shapes, market values on the same two keys as positions.

## The four runs

| # | partitions | mode | what it isolates |
|---|---|---|---|
| 1 | 2 | **live** — producer and consumer together | steady-state behaviour |
| 2 | 2 | **backlog** — queue all data first, then consume | capacity, no producer CPU competition |
| 3 | 4 | live | partition effect, steady state |
| 4 | 4 | backlog | partition effect, capacity |

Live vs backlog matters because they measure different things: live shows what
the pipeline does when fed in real time; backlog drain shows what it *can* do.
Phase 21 showed local live runs are producer-bound, so both are needed.

## Checks — every run, every output

1. **Completeness** — every input trade accounted for; `sum(accounts) == symbol total`
2. **Correctness** — positions equal deduped trade counts; market value equals
   position × price
3. **No out-of-sequence publishing** — per key, `as_of` never goes backwards,
   no older price used after a newer one, no consumer left on a superseded value
4. **Staleness** — p50/p90/p99/max, in ms

## Status output

One table per run, standard format:

| Condition | in trades/s | in prices/s | out positions/s | out MV/s | Parallelism | Utilization % | Total $/hr | Flink KPU | Flink $/hr | BackPressure | Kafka partitions | Kafka $/hr |

Plus correctness/ordering/staleness alongside.

## One thing to expect, stated up front

**A 250ms tumbling window on prices is the configuration Phase 21 measured as
producing ~2-3 second stale market values** — 0% exact, because the window
discards the newest tick before it is used. So the "market value == position ×
FINAL price" check is expected to FAIL on all four runs.

That is not a new defect and not a reason to stop: staleness will be reported as
a distribution rather than pass/fail, so the four runs stay comparable and the
window's cost is quantified at each partition count. Positions, completeness and
ordering should all pass regardless.

If the intent is to see the window's behaviour across partition counts, this
plan does that. If the intent is a correct pipeline, the Phase 21 finding
applies: reduce the output, not the input.

## Open question carried in

Salt is a parallelism device and should have nothing to do with correctness, yet
DataStream at `price.salt.factor=1` published 490 of 500 keys with an
out-of-order price while salt=8 published none. The event-time guard in the
market-value operators should have prevented that either way. **Unexplained** —
worth resolving inside this phase since run 1 exercises exactly that path.

## Method rules

* Local first; no cloud in this phase.
* One run, recorded and committed, before the next.
* Verify launches by log content, never a process count.
* Report what fails as prominently as what passes.
