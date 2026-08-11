# Phase 22 — verify the working config on AWS and Confluent

**Prerequisite:** Phase 21 established the working configurations locally and
proved the old ones are dominated. This phase confirms they hold at real scale
and closes the cross-platform comparison.

---

## Why this phase exists

Two things are known to be wrong in the published numbers:

1. **Every AWS SQL figure was measured on the broken config.** The Phase 20
   scripts set `sql.price.conflate.ms=250` with `mv.emit.interval.ms=0` --
   input reduce ON, output reduce OFF, backwards on both. SQL was publishing 3x
   more records than necessary and doing the work to produce them, so its
   throughput is understated and the DataStream-vs-SQL gap is overstated.
2. **AWS DataStream runs had `price.salt.factor=1`**, the vulnerable default.
   Final values were correct, but in-flight ordering was never checked -- the
   ordering check did not exist when those ran.

## Steps

### 1. Fix the shipping default (code, free)
`price.salt.factor` defaults to 1, which disables the event-time guard on the
DataStream path. Change to 8 and re-verify locally.

### 2. AWS correctness at scale (~$3, in flight at phase start)
Both working configs, 200 symbols, salted feed, 16 partitions. Confirms the
local result and runs the ordering check on AWS for the first time.

### 3. AWS throughput, re-measured on the working config (~$4)
SQL at P=20 with the correct settings, against the 6,106 rec/s already on record
for the broken config. Same rig, same feed, one variable. This is what decides
whether the DataStream-vs-SQL gap survives.

### 4. Confluent (~$3, needs work first)
* `confluent/sql/working/statement_set.sql` is written but **terraform deploys
  the dml/ files individually and never references a statement set** -- it needs
  a new resource before it is reachable.
* **Unverified: whether Confluent Cloud exposes an output reduce at all.** On AWS
  that is the upsert-kafka option `sink.buffer-flush.interval`; Confluent uses
  managed tables with `changelog.mode` / `key.format` / `value.format`, and the
  docs did not confirm an equivalent. Test with a CREATE TABLE before assuming.
* If it is not expressible, that is the finding: the fix is only half portable,
  which is a more useful platform difference than a throughput number.

## Pre-declared interpretation

* **SQL on the working config beats 6,106 rec/s materially** -> the published
  DataStream-vs-SQL gap is overstated and must be restated.
* **No material change** -> the gap stands, and the conflation setting affects
  correctness but not throughput.
* **Confluent cannot express the output reduce** -> report as a platform
  capability difference, not a performance result.

## Method rules carried forward

* Test BOTH engines; three conclusions changed in Phase 21 once the second was
  measured.
* Local first for any code change; AWS only for what needs real Kafka.
* One condition, recorded, committed, then the next.
* Verify launches by log CONTENT, never a process count or exit code.
* Unconditional teardown plus an independent watchdog.
