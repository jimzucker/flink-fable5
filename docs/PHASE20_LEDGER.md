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
| SQL, materializer ON (control) | **FAIL** | | 30/1000 and 6/200 outside a 2s tolerance — WORSE than without it |
| SQL, materializer OFF | **FAIL** | | 20/1000 and 4/200 — same behaviour as the control, so the materializer is NOT the cause |

## Throughput / scaling — fixed backlog, one method

| condition | total rec/s | notes |
|---|---|---|
| SQL P=20 | **6,106** | fixed backlog, app stopped during build |
| SQL P=40 | **10,363** | rerun w/ 8-min backlog, sampled mid-drain -> **1.70x vs P=20** |
| DataStream P=20 | **12,487** | fixed backlog |
| DataStream P=40 | **28,357** | rerun w/ 8-min backlog -> **2.27x vs P=20** |

## Superseded (kept only to show what was replaced and why)

| number | why it is not used |
|---|---|
| SQL 1.17x / "+17%" | per-subtask metric divided by parallelism |
| SQL 0.98x | same error, my own version |
| SQL 1.74x (7,690 -> 13,405) | correct metric, but unequal backlogs -- superseded by the fixed-backlog rows above |
| DataStream 11,421 / 35,058 | P=20 was caught up, so it measured the generator, not capacity |
| Materializer removal (~1.9x) | config produces WRONG results — dropped, do not quote |

## Method flaw found mid-run (2026-08-09)

The fixed-backlog design samples after a 3-minute settle. That assumes the drain
outlasts settle+sample. It does for slow configs and NOT for fast ones: SQL P=40
cleared a 3-minute backlog during the settle window, so the 7-minute sample
measured an idle pipeline — prices source at exactly 0/s.

P=20 rows are unaffected: they were still draining throughout.

**Fix for any rerun:** sample from the moment the app reaches RUNNING (no settle),
and size the backlog to the FASTEST config, not the slowest. A 3-minute build at
~10k rec/s is roughly 2M records, which DataStream P=40 clears in well under a
minute.

## Correction: the materializer was not the cause (2026-08-09)

Earlier this phase I recorded "removing the materializer FAILS correctness" from
a single arm. The control — SQL **with** the materializer, exactly as shipped —
then failed too, and by MORE:

| config | wrong (account) | wrong (ticker) |
|---|---|---|
| materializer ON (control) | **30 / 1000** | **6 / 200** |
| materializer OFF | 20 / 1000 | 4 / 200 |

**The causal claim was wrong.** Both configurations behave the same, so the
materializer does not explain the failure. I asserted cause from one arm before
its control existed, which is the same error pattern as the metric bug: a
confident conclusion drawn from a measurement that could not distinguish the
hypotheses.

**What the numbers actually say.** SQL produces ZERO exact matches: ~93% of keys
land inside the 2,000ms lag tolerance and a ~3% tail falls outside it. DataStream
on the same feed produced 945/1000 EXACT. That is a systematic latency
difference — SQL's conflate + tumble + checkpoint path is simply further behind
the final price — not evidence that SQL computes the wrong arithmetic.

**Still open:** whether the ~3% tail is genuine staleness or an over-strict
threshold. A tolerance sweep (2s / 5s / 10s) would separate them, and needs no
new infrastructure beyond one run. Until then, neither "SQL is incorrect" nor
"SQL is correct" is established — only that SQL lags materially more than
DataStream.

## Scaling result (2026-08-09) — DataStream converts parallelism better

| API | P=20 | P=40 | scaling |
|---|---|---|---|
| **DataStream (AWS)** | 12,487 | 28,357 | **2.27x** |
| **SQL (AWS)** | 6,106 | 10,363 | **1.70x** |

Same feed, same fixed-backlog method, same cost per rung ($3.67 -> $4.22/hr).

**Both scale. DataStream scales better**, and the gap WIDENS with parallelism:
2.05x faster at P=20, **2.74x faster at P=40**.

DataStream's 2.27x is superlinear, which normally signals a confound. Here it is
consistent with the utilization data: at P=20 it ran at 35% busy, so P=40
relieved a constraint rather than adding pure capacity. Treat "better than
linear" as approximate; the DIRECTION is solid.

**Utilization at every rung says both are over-provisioned:**

| API | P | busy | bp | effective slots |
|---|---|---|---|---|
| DataStream | 20 | 35.3% | 4.0% | ~7 of 20 |
| DataStream | 40 | 17.4% | 4.3% | ~7 of 40 |
| SQL | 20 | 44.3% | 14.9% | ~9 of 20 |
| SQL | 40 | 31.1% | 1.7% | ~12 of 40 |

DataStream puts ~7 slots to work at BOTH 20 and 40 — it got 2.27x more
throughput from the same effective compute, so the gain came from relieving a
constraint, not from using more capacity. Neither engine gets near the 60-70%
target; every rung here is over-provisioned for this load.

Supersedes the Phase 19 claim that SQL fails to scale, and the earlier Phase 20
figure of 1.74x measured under live load.
