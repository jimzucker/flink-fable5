# How we know the numbers are right

One page, per the lifecycle: *validate calculations with simple inputs, verify
completeness, reproducibility; behavior must be deterministic.*

## 1. Hand-computed golden test (simple inputs)

`GoldenPipelineTest` feeds 6 fixed trades (including 1 duplicate) and 2 final
prices through the **real operator chain** and asserts every output against
numbers computed by hand first:

| Check | Expected |
|---|---|
| Duplicate T-1 dropped | 5 trades applied |
| ACC-001\|AAPL / ACC-002\|AAPL / ACC-001\|MSFT | 110 / 30 / −30 |
| AAPL / MSFT ticker position | 140 / −30 |
| MV ACC-001\|AAPL (110 × $155.50) | 17,105.00 |
| MV AAPL (140 × $155.50) | 21,770.00 |
| MV MSFT (−30 × $410.25) | −12,307.50 |

## 2. Operator unit tests (Flink test harnesses)

14 tests, `make test`: dedup (first-wins, duplicate injection), running sums,
cross-account aggregation, join semantics (no MV before a price arrives; a
price tick re-values every holder), and the extreme-price case —
999,999 shares × $10¹³ computed exactly with no overflow (perf Case 2).

## 3. Live validation against the running stack

`make validate` pauses the generator, lets the pipeline drain, dumps all six
topics, then **recomputes every output independently in Python** from the raw
trades/prices and compares:

- **Dedup**: duplicates in input = input − distinct trade_ids (e.g. 20,210
  records, 959 duplicates, 19,251 applied)
- **Reproducibility**: recomputed positions == pipeline output, every key
- **Completeness**: Σ account positions == ticker position, every ticker
- **Market value**: final MV == final position × final price, exact `Decimal`
  comparison (no tolerance)

## 4. Why results are deterministic

- Generator content is seeded (`generator.seed`) — same seed, same trades
- Positions are commutative sums of signed quantities: arrival order across
  partitions cannot change the final state
- Money is exact: prices are long cents, MV is BigDecimal — no float drift
- Dedup is first-wins on trade_id with keyed state, so replays are idempotent
- `GoldenPipelineTest.sameInputTwice_identicalResults` asserts identical output
  across repeated runs

Under concurrent price/position updates, *intermediate* MV snapshots depend on
interleaving, but the *final* state after drain is always
position × latest price — which is exactly what `make validate` proves.
