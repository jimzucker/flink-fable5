# Phase 21 — results

**Question:** is SQL's market value wrong, or just late?

**Answer: SQL is correct.** The staleness came from one setting, and the fix
gives exact values *and* fewer writes than the configuration we were running.

Everything below was measured on the local Docker rig at **$0**. The AWS run for
this phase produced nothing usable (my own topic-recreation bug); every finding
here came from the laptop.

---

## 1. The cause: reducing the INPUT instead of the OUTPUT

`sql.price.conflate.ms=250` puts a 250ms tumbling window over the price stream
and keeps one tick per window. That **discards the newest tick before it is
used**, so the published market value can never be current.

Isolated with one variable, identical input (440,000 records):

| config | output records | staleness p50 | exact | result |
|---|---|---|---|---|
| input reduce 250ms *(what AWS benchmarks ran)* | 55,389 | 2,929ms | 0% | FAIL |
| **output reduce 1000ms** | **18,386** | **0ms** | **100%** | **PASS** |
| output reduce 250ms | 38,773 | 0ms | 100% | PASS |
| no reduce at all | 439,623 | 0ms | 100% | PASS |

**The fix wins on both axes** — 3x fewer writes than the current config AND
exact. The old setting is dominated, not a trade-off.

**Rule: reduce the OUTPUT, never the INPUT.** An output reduce publishes the
newest value less often. An input reduce throws the newest value away.

## 2. Three of my hypotheses were wrong, all disproven locally for free

| hypothesis | verdict |
|---|---|
| Windows never close when a symbol goes quiet | **wrong** — quiet symbols catch up in ~4s, not 120s |
| The source idle timeout is the missing fix | **wrong** — defaults to 5000 and was always on; shortening it made staleness *worse* |
| A shorter window improves freshness | **wrong** — 100ms was 3x staler than 250ms for the same record count |
| Removing the materializer caused it | **wrong** (Phase 20 carry-over) — the control failed by more |

## 3. Ordering is clean — the concern that prompted this phase

Across ~1.1M published records, every configuration:

| check | result |
|---|---|
| positions published out of order | **0** |
| out-of-order prices used for output (incl. intermediate) | **0** |
| consumers left holding a superseded value | **0** |

...with one exception, found only because both engines were finally tested:

## 4. DataStream ships in a vulnerable default

`price.salt.factor` defaults to **1**, which disables `LocalPriceConflator` —
the component carrying the event-time guard.

| config | out-of-order prices used | staleness | six checks |
|---|---|---|---|
| salt 1 (**shipping default**) | **490 of 500 keys** | 0ms | PASS |
| salt 8 | **0** | 0ms | PASS |

Final values are correct either way, so **the six checks pass in both cases** —
the violation is entirely mid-stream and was invisible until the ordering check
existed. A consumer watching updates sees the value move backwards, then forward.

## 5. Working configurations

See [WORKING_CONFIG.md](WORKING_CONFIG.md). In short:

* **SQL:** `sql.price.conflate.ms=0`, `mv.emit.interval.ms=1000`
* **DataStream:** `price.salt.factor=8`, `mv.emit.interval.ms=1000`

Both: 100% exact, 0ms stale, zero ordering violations, all six checks pass.

**The SQL defaults were already correct** — the Phase 20 benchmark scripts
overrode them with input-reduce ON and output-reduce OFF, backwards on both
counts. So every AWS SQL figure was measured on the dominated config.

## 6. Local cannot measure scaling

Fixed-backlog drain with the generator stopped:

| P | drain rec/s | busy% | backpressure% |
|---|---|---|---|
| 2 | 4,781 | 31.1 | 0.0 |
| 4 | 5,849 | 31.9 | 0.0 |

Busy% should be high during a drain. At 31% with zero backpressure the pipeline
is **source-bound**: the single-broker local Kafka caps at ~5,000-6,000 rec/s and
Flink is never the constraint. Proposing local scaling tests before establishing
the rig's ceiling was an error.

Local is right for correctness, ordering, mechanism and config trade-offs.
Scaling needs real Kafka.

---

## Open, carried to Phase 22

1. `price.salt.factor` should default to **8**, not 1.
2. AWS SQL throughput was measured on the broken config and understates SQL.
3. AWS DataStream runs had `price.salt.factor=1`, so they likely carried the same
   in-flight ordering violations — undetected, as the check did not exist then.
4. Confluent: the working statement set is written but not wired into terraform,
   and whether the output reduce is expressible there is unverified.

## What carries forward

* Test BOTH engines. Three separate conclusions changed once the second engine
  was measured.
* A test that cannot fail is not evidence — the six checks passed on a
  DataStream config publishing 490 out-of-order prices.
* Establish what the RIG can sustain before measuring the system on it.
* Reduce the output, never the input.
