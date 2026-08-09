# Phase 20 results ledger

One row per completed condition, written and committed as soon as it finishes.
Nothing here is re-run because something else failed.

**Method is fixed and must not change between rows.** Throughput = total rec/s
via `scripts/phase20_totals.py` (per-subtask average x subtask count).
Correctness = six checks with the bounded lag tolerance. Mixing methods is what
produced the retractions.

## Correctness

| condition | result | stale ticks | notes |
|---|---|---|---|
| DataStream, salted feed | **PASS** | 461,523 dropped | 945/1000 keys exact; guard is load-bearing |
| SQL, materializer ON | **NO RESULT** | | validator OOMed (512MB task, 48-partition fetch buffers) — fixed, needs one rerun as the CONTROL |
| SQL, materializer OFF | **FAIL** | n/a (SQL mode) | **20/1000 and 4/200 market values wrong** under the bounded 2000ms tolerance |

## Throughput / scaling — fixed backlog, one method

| condition | total rec/s | notes |
|---|---|---|
| SQL P=20 | *pending* | |
| SQL P=40 | *pending* | |
| DataStream P=20 | *pending* | |
| DataStream P=40 | *pending* | |

## Superseded (kept only to show what was replaced and why)

| number | why it is not used |
|---|---|
| SQL 1.17x / "+17%" | per-subtask metric divided by parallelism |
| SQL 0.98x | same error, my own version |
| SQL 1.74x (7,690 -> 13,405) | correct metric, but unequal backlogs -- superseded by the fixed-backlog rows above |
| DataStream 11,421 / 35,058 | P=20 was caught up, so it measured the generator, not capacity |
| Materializer 1.89x (13,405 -> 25,387) | same parallelism so the metric is sound, but correctness untested |

## Materializer verdict (2026-08-09)

Removing `SinkUpsertMaterializer` (`table.exec.sink.upsert-materialize=NONE`)
**failed correctness**: 20 of 1,000 account market values and 4 of 200 ticker
market values were wrong, against a bounded 2,000ms lag tolerance — so these are
not "slightly behind", they fall outside a window far wider than the 250ms
conflation interval.

That is the predicted failure mode. The materializer exists because the planner
cannot prove the upsert key matches the sink primary key; deleting it removes
the operator that repairs out-of-order updates, and V1 measured that disorder is
the majority case on this feed (461,523 of 866,000 prices).

**So the ~1.89x speedup is NOT claimable.** It buys throughput by dropping
correctness — the exact "fast wrong answer" the standing rule exists to catch.

**Missing control:** the same test WITH the materializer did not produce a
result (validator OOM, since fixed). Until that runs, the alternative
explanation — that a 2,000ms tolerance is simply too strict for this SQL
topology, materializer or not — is not excluded. One short run closes it.

**The real fix is unchanged and does not involve this flag:** resolve the upsert
key (the `GROUP BY CONCAT(...)` already used on position-by-account-ticker) so
the planner never inserts the operator. That yields the throughput AND the
correctness instead of trading one for the other.
