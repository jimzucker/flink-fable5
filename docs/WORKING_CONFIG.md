# What works and what does not

Measured on the local rig. Every row was tested, not inferred.

---

## WORKS — use these

### SQL

```
pipeline.mode=sql
sql.price.conflate.ms=0        # NO input reduce (no tumbling window on prices)
mv.emit.interval.ms=1000       # output reduce: publish at most 1/sec per key
position.emit.interval.ms=500  # output reduce
```

| check | result |
|---|---|
| Final prices | correct |
| Final positions | correct, 0 mismatched |
| Final market values | **100% exact, 0ms stale** |
| Out-of-order positions published | 0 |
| Out-of-order prices used for output | 0 |
| Consumers left on a stale value | 0 |
| Six-check validation | **PASS** |

Published 157,164 records from 442,200 input (~2.8:1).

### DataStream

```
pipeline.mode=datastream
price.salt.factor=8            # input reduce ON (max by event time)
mv.emit.interval.ms=1000       # output reduce
position.emit.interval.ms=500  # output reduce
```

| check | result |
|---|---|
| Final market values | **100% exact, 0ms stale** |
| Out-of-order positions published | 0 |
| Out-of-order prices used for output | 0 |
| Six-check validation | **PASS** |

---

## DOES NOT WORK — avoid these

### SQL with an input reduce (tumbling window on prices)

```
sql.price.conflate.ms=250      # BAD: windows the INPUT
mv.emit.interval.ms=0          # BAD: no output reduce
```

**Final market values are ~2-3 seconds stale and never correct themselves.**
p50 1,968ms, max 2,742ms, 0% exact. Two of six checks FAIL.

It also publishes **3x MORE** records than the working config (55,389 vs 18,386),
so it is worse on both correctness and volume. There is no case for it.

Shrinking the window makes it worse, not better: at 100ms staleness tripled to
p50 6,295ms for the same record count.

**This is the configuration every AWS SQL benchmark ran on.**

### DataStream with no input reduce

```
price.salt.factor=1            # BAD: no reduce, so no max-by-event-time
```

Final values are correct and staleness is 0ms, but **490 of 500 market-value keys
publish an out-of-order price mid-stream** — an older tick used after a newer one.
A consumer watching updates sees the value move backwards, then forward again.

Passes the six checks (which only assert the FINAL value), so this defect is
invisible without the ordering check.

**`price.salt.factor` defaults to 1, so this is the shipping default.**

---

## The rule

**Reduce the OUTPUT, never the INPUT.**

* An **output reduce** (emit interval) publishes the newest value less often —
  the published value is always current.
* An **input reduce** must be *max by event time*. SQL's tumbling window
  discards the newest tick instead, so the result can never be current.
  DataStream's reduce does it correctly — but only when it is switched on.

---

## Open items

1. **`price.salt.factor` should default to 8**, not 1. The DataStream path ships
   in the configuration that publishes out-of-order prices.
2. **AWS SQL figures were measured on the broken config** (input reduce on,
   output reduce off), so they understate SQL.
3. **AWS DataStream runs had `price.salt.factor=1`**, so they likely carried the
   same in-flight ordering violations — undetected, as the ordering check did not
   exist when they ran.
