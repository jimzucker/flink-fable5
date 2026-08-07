# Phase 18 — correctness-gated performance on a realistic market

**Scope: Confluent only.**

## The rule this phase exists to enforce

> **Verify correctness and completeness before chasing performance — and when
> you run at volume, verify correctness AT THAT VOLUME before claiming the
> number. Fast only counts if the numbers are correct.**

Phases 16 and 17 produced dozens of throughput figures, four withdrawn
conclusions and several re-runs. **The validation suite never successfully ran
in either phase.** It was attempted once, crashed on stale credentials, and the
failure went unnoticed because nothing gated on it.

Meanwhile changes shipped that carry genuine correctness risk, each argued safe
from first principles and none proven:

| Change | Risk |
|---|---|
| `sql.tables.scan.idle-timeout=5000` | forces watermarks past idle partitions — can close event-time windows early and **drop late ticks** |
| write-time key salting | a symbol's records now span partitions — cross-partition arrival order can shift **which window a record lands in** |
| `GROUP BY CONCAT(...)` | changed the aggregation key |
| `COALESCE` on primary keys | changed null handling |

A faster wrong answer is worth nothing.

## The dataset — a realistic trading day

Ten uniformly-loaded tickers was the least realistic shape available: every
symbol equally busy, so "one worker per key" looked like parallelism and key
starvation dominated every result. US equities are ~8,000–11,000 tradeable
symbols on a Pareto curve, with volume concentrated in a small head.

**Standard dataset from here on:**

```
--generator.tickers        3000
--generator.distribution   zipf
--generator.zipf.alpha     1.0
--generator.ipo.share      0.30
--generator.ipo.ticker     2999
```

Validated offline: IPO symbol 38.1% of tape, top 10 53.9%, top 100 72.3%,
top 500 85.4%, all 3,000 symbols trading. A normal Pareto market **plus one hot
listing** — the IPO day.

## Work

### 1. Streaming validator (prerequisite — nothing else counts without it)

`scripts/confluent_validate.py` buffers every record into a list (`dump_topic`),
so it **cannot validate a volume run**. At 19M records that is gigabytes.

Rewrite with bounded memory:
* stream `trades` → recompute positions incrementally (trades are the small side)
* stream `prices` → retain only latest-per-symbol (3,000 entries)
* same six checks, no full materialisation

Six checks, each an independent recomputation with exact decimal arithmetic:
dedup, positions by account+ticker, positions by ticker, cross-aggregation
completeness, MV by account, MV by ticker.

### 1b. Make the arithmetic trivial

Correctness checking is far easier when the numbers are simple enough to verify
by hand rather than by a second program.

```
--generator.qty.override           1     # every trade is 1 share
--generator.price.cents.override   100   # every price is $1.00
```

With those two settings the expected outputs become arithmetic anyone can do in
their head:

| Output | Expected value |
|---|---|
| position by account+ticker | **count of deduped trades** for that key |
| position by ticker | sum of its accounts' counts |
| MV by account+ticker | **= position, in dollars** (position x $1.00) |
| MV by ticker | = ticker position, in dollars |
| sum over accounts | must equal the ticker position exactly |

Any rounding error, float creep, double-count, dropped record or duplicate that
slipped through shows up as an **off-by-N a human can see** — no need to trust a
recomputation program that could share a bug with the pipeline.

Run the correctness pass with simple numbers; run the performance pass with
realistic quantities and prices (they exercise the decimal path). Both must
validate, but simple numbers make a failure legible instead of merely detected.

### 2. Validate at volume, then measure

Order is the point:

1. seed the realistic dataset at volume
2. run the pipeline
3. **validate — six checks must pass**
4. only then record throughput

Any number reported as *"X rec/s, six checks passed"* — or not reported.

### 3. Re-test what the ten-ticker dataset made meaningless

Nearly every ceiling found so far traced back to having ten keys. At 3,000
symbols the ticker-keyed stages have thousands of keys and
`position-by-account-ticker` has ~15,000.

| Question | Why it needs re-testing |
|---|---|
| **Does it scale with compute?** | "Saturates at ~10 CFU" was measured with 10 keys. The pool drew only 4–6 of an allowed 10 — it declined compute it had. |
| Is salted conflation still worth 2.66×? | It manufactured keys we did not have. With 3,000 real keys it may be unnecessary overhead. |
| Is `mv-by-ticker` still a bottleneck? | 10/20 slots at ten tickers; 3,000 keys removes the starvation entirely. |
| Confluent end-to-end latency | never measured on the corrected config. |
| Does adaptive keying still help? | the IPO hotspot is real at any cardinality — a hot symbol is still one partition. |

## Method rules carried forward

* **Correctness gates performance** — at the volume being claimed.
* Compare **rates**, not drain times, unless backlogs are identical.
* **Purge tables between runs** — leftover backlogs silently contaminate.
* `sent_records` is **not** records-processed; use seeded ÷ drain duration.
* Sample **CFU during the run** — Confluent telemetry dies with the pool.
* Run `scripts/utilization_audit.py` before trusting a measurement.
* Two runs minimum near a close call — run-to-run variance is ~25%.
* When two conditions that should differ return the **same** number, suspect the
  harness.
