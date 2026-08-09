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
| SQL, materializer OFF | **FAIL — dropped** | | produced wrong market values; not a usable config, not pursued |

## Throughput / scaling — fixed backlog, one method

| condition | total rec/s | notes |
|---|---|---|
| SQL P=20 | **6,106** | fixed backlog, app stopped during build |
| SQL P=40 | *pending* | |
| DataStream P=20 | **12,487** | fixed backlog |
| DataStream P=40 | *pending* | |

## Superseded (kept only to show what was replaced and why)

| number | why it is not used |
|---|---|
| SQL 1.17x / "+17%" | per-subtask metric divided by parallelism |
| SQL 0.98x | same error, my own version |
| SQL 1.74x (7,690 -> 13,405) | correct metric, but unequal backlogs -- superseded by the fixed-backlog rows above |
| DataStream 11,421 / 35,058 | P=20 was caught up, so it measured the generator, not capacity |
| Materializer removal (~1.9x) | config produces WRONG results — dropped, do not quote |
