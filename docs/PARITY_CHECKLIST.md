# Parity checklist — gate before any comparison claim

The comparison only means something if each cell is the *best tuned* version of
itself and the differences between cells are the ones we intend to measure.
This file is the gate. Nothing gets published from a run where a row below is
out of sync without the asymmetry being named in the claim itself.

Three cells:

| Cell | What it isolates |
|---|---|
| **DataStream (AWS)** | the best this workload can be expressed |
| **SQL (Confluent)** | SQL, on a managed-SQL-first platform |
| **SQL (AWS)** | the same SQL, different platform — isolates platform from language |

**SQL (AWS) and SQL (Confluent) must run the same query text.** Any difference
between those two cells that is not platform is a bug in the experiment.

## Status

| Tuning | DataStream (AWS) | SQL (Confluent) | SQL (AWS) | In sync? |
|---|---|---|---|---|
| Fused single job (no intermediate topics) | yes | yes (`EXECUTE STATEMENT SET`) | yes | yes |
| Exactly-once delivery | yes | yes | yes | yes |
| Upsert key matches sink primary key | n/a | 3 of 4 statements | 3 of 4 statements | yes |
| CR-1 output cadence cap | yes (per-key timers) | **NOT POSSIBLE** | yes (`sink.buffer-flush.interval`) | **NO** |
| Key salting on narrow keys | yes (`LocalPriceConflator`) | yes (salted `conflated` CTE) | yes (same CTE) | yes |
| Sink partitions / buckets | 48 topic partitions | 6 buckets | 48 topic partitions | **NO** |

## Open gaps

### 1. CR-1 cannot be expressed in Confluent SQL

AWS SQL implements the CR-1 cadence cap with the upsert-kafka connector's
`sink.buffer-flush.interval` (500 ms positions / 1000 ms market values), which
buffers per key and emits the newest value at most once per interval.

Confluent Cloud Flink **rejects that option**. Submitting it returns
`Unsupported options found` and the engine's own supported-options list has no
sink buffering or cadence control of any kind:

```
Unsupported options: sink.buffer-flush.interval, sink.buffer-flush.max-rows
Supported options: changelog.mode, connector, error-handling.*, kafka.*,
                   key.*, late-handling.mode, scan.bounded, ...
```

The SQL-level workaround (`confluent/sql/optimized/cr1_cadence.sql`, CUMULATE
windows) was tried and failed: size ÷ step windows per key (1 day / 0.5 s =
172,800) starved the outputs and pushed latency to 158–235 s.

**Consequence.** A plain business requirement — "don't update a number on a
screen faster than a human can read it" — is one connector option on AWS SQL
and is currently not expressible on Confluent SQL. That is a capability
finding in its own right and belongs in the writeup.

**For measurement:** run the throughput comparison with CR-1 *off* on both SQL
cells, so the number compares engines rather than comparing "emits every
update" against "emits at most 2/sec/key". Report CR-1 separately as a
capability row, not as a throughput number.

### 2. Salting — now applied to all three (salt factor 8)

Resolved. All three pipelines carry two-phase (local-global) conflation on the
price path at salt factor 8, giving 80 shard keys over 10 symbols against 48
partitions:

* **DataStream** — `LocalPriceConflator` keys on `(symbol, salt)` ahead of the
  narrow `keyBy(ticker)`, keeps the newest tick per shard, and the existing
  `keyBy(symbol)` reduces the candidates. `price.salt.factor <= 1` adds no
  operator, preserving the baseline topology.
* **Both SQL cells** — the same salted `conflated` CTE. Both phases stay
  `ROW_NUMBER` rather than `GROUP BY`, because dedup preserves the time
  attribute and keeps `wt` orderable downstream; a `GROUP BY` reduction strips
  it, and after `TUMBLE` there is no other time attribute to recover. That is
  why the standalone `salted_conflate.sql` had to use the lexicographic
  `LPAD`/`CONCAT`/`MAX`/`SUBSTRING` encode — it had already reduced with
  `GROUP BY`. Verified by `EXPLAIN`: `Upsert key: (symbol)` survives both
  phases and no new warnings appear.

The salt must vary per RECORD. Hashing the symbol yields a constant per symbol
and manufactures no parallelism whatsoever.

### Historical note — the claim that was wrong

Correcting an earlier statement in this repo's notes: the Phase 14 "+85% from
salting" figure came from `confluent/sql/optimized/salted_conflate.sql`, a
**standalone conflation-only statement**. It was never folded into the fused
`statement_set.sql`, and there is no salting in `SqlPipeline.java` or in the
DataStream job either (`grep -i salt src/main/java` returns nothing).

So salting is absent from all three end-to-end pipelines, and the earlier note
that "Confluent got salting and AWS did not" is wrong at the pipeline level.
The three cells are consistent with each other today — but all three are
consistently *un*-tuned on this axis.

This matters most for the scaling lens. Four of six DataStream `keyBy` stages
sit on a 10-value key space (`PositionPipeline.java` lines 99, 120, 131, 143),
including the heaviest output — market value by account+ticker keys on
`ticker` alone because it has to meet the price stream. Parallelism above ~10
cannot help those operators. Any linear-scaling claim needs to say whether it
was measured below or above that ceiling.

### 3. Partition counts differ — and this one caps scaling

Set to **48 on both** (was AWS 16 / Confluent 6).

**The rule: partitions >= the highest parallelism under test.** A Flink source
cannot read a topic with more parallelism than it has partitions, so every
subtask past the partition count sits idle holding compute you are paying for.
Testing a P=40 rung against 16 partitions measures the partition count, not the
platform. 48 covers P=40 with headroom.

Historical values, both of which throttled runs:

| | Source topics | Sinks |
|---|---|---|
| AWS | 16 (`topics_partitions` default) | 16 |
| Confluent | **6 buckets** | **6 buckets** |

Buckets *are* partitions on the backing topic, and **a Flink source cannot read
a topic with more parallelism than it has partitions**. Confluent's `prices` at
6 buckets caps price reading at 6 subtasks no matter how many CFUs the pool
has. That is a ceiling sitting *underneath* the key-starvation one, and it
means every baseline Confluent scaling measurement is confounded — the
`prices-bulk48` topic built for the drain test has 48 buckets precisely to lift
it, but the baseline tables did not.

**Cost of fixing it: zero on Confluent.** The deployed cluster is
`{"kind": "Basic", "max_ecku": 50}`. Basic clusters on the eCKU model (orgs
created after 2024-04-16, which this one is) have **no per-partition charge** —
billing is eCKU, ingress/egress and storage. Per-partition pricing existed only
in the legacy Base+Partitions model.

At the 48 setting, across 6 topics = 288 partitions:

| | Per-partition cost | 288 partitions (48 x 6 topics) |
|---|---|---|
| AWS MSK | $0.0015/partition-hr | **~$0.43/hr, ~$315/mo** |
| Confluent Basic (eCKU) | $0 | **$0** |

So partition count is a **cost knob on AWS and free on Confluent**, and this is
a finding in its own right rather than a footnote: **on AWS, using your compute
efficiently costs extra money.** Reaching full utilisation at P=40 requires
buying 288 partition-hours; the identical change on Confluent is free. Any TCO
comparison that prices compute but not the partitions needed to keep that
compute busy is understating AWS.

Matched UPWARD rather than down. Matching downward would save AWS money but cap
both sides and hide the scaling behaviour being measured.

## Verifying the upsert-key row

`EXPLAIN` each INSERT and read the `== Warnings ==` block. Clean looks like
`Upsert key: (<col>)` carried to the sink with max `State size: medium`.
Broken looks like an upsert key at the aggregate, nothing at the sink, and a
`State size: high` operator — that high-state operator is the correction
operator, and it is what also trips the separate no-TTL warning. One cause,
two warnings.

Current: `position-by-ticker`, `mv-by-ticker`, `position-by-account-ticker`
clean. `mv-by-account-ticker` still warns — its left upsert key is `acct_key`
but it joins on `ticker`, a non-key column. Fixing that needs a temporal join
against a versioned table, which cannot be expressed against a CTE inside a
fused statement set. Fusion and upsert-key preservation genuinely conflict for
that one output; it is an engine limitation, not a coding mistake.
