# Optimized SQL variants (Phase 12)

The baseline pipeline in `../dml/` mirrors the Java topology one-for-one:
dedup and conflation write **intermediate Kafka topics**, which the four
output statements then re-read. That is six independent jobs and two extra
Kafka round-trips on every path — a faithful translation of the DataStream
graph, but not how you would write it for latency.

These variants test how much of the latency gap is the translation rather
than the language:

| File | Change | Statements | Intermediate topics |
|---|---|---|---|
| `../dml/*.sql` (baseline) | one statement per operator | 6 | 2 |
| `inline/*.sql` | dedup + conflation inlined as subqueries | 4 | 0 |
| `statement_set.sql` | all four outputs in ONE job | 1 | 0 |

`statement_set.sql` is the structural equivalent of the Java job: a single
job graph, one source scan, operators handing off in memory, four sinks.
If Confluent Cloud accepts `EXECUTE STATEMENT SET`, this is the fair
comparison against DataStream — everything else is comparing a fused job
to a deliberately un-fused one.
