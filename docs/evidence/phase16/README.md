# Phase 16 evidence

Raw artefacts behind every phase-16 claim. Captured while the stacks were live;
CloudWatch data is exported rather than screenshotted because the dashboard
disappears at teardown and a JSON series can be re-plotted, re-checked, and
diffed. Console screenshots cannot.

| File | What it proves |
|---|---|
| `01_utilization_audit_pass.log` | The deployed AWS config could actually use its compute: 48 partitions >= parallelism 20, salted conflation 80 keys, 0% of parallelism unreachable. |
| `02_drain_all_zeros_crashloop.log` | **The most important file here.** A complete drain measurement returning 0.0 records/sec on every sample, from a job that was restart-looping on the transaction-timeout bug. Status `RUNNING`, 100% busy, zero backpressure, no `FAILED` transition — every signal individually plausible. This is what an untrustworthy measurement looks like. |
| `03_drain_salted_982s.log` | AWS DataStream salted: 143.68M records drained in 982s => ~148,000 rec/s, at 92% backpressure (bottleneck downstream of source, so capacity not feed rate). |
| `04_drain_unsalted_2613s.log` | Same config, `price.salt.factor=1`: 2613s. Salting is **2.66x** on DataStream — the opposite of what was predicted. |
| `05_seed_12tasks_140M.log` | Seed provenance: 12 generator tasks, ~11.7M prices each. Backlog sizing must beat `drain_rate x (blind_spot + plateau)`, which two earlier seeds failed. |
| `06_audit_FAIL_historical_confluent_6buckets.log` | The audit FAILS the historical Confluent config (6 buckets, 20 CFUs, unsalted) that produced the "Confluent cannot scale" reading. |
| `07_audit_PASS_corrected.log` | The same audit passing the corrected config. |
| `08_cloudwatch_metrics_3h.json` | 135-point/1-min series for 10 metrics across the whole run window — the source for any chart in the README, deck or article. |
| `09_confluent_unsupported_options.txt` | Confluent's own error listing every supported table option, with no sink-cadence control among them. CR-1 is inexpressible there. |
| `10_confluent_cluster_type.txt` | Cluster is Basic/eCKU, so partitions are free — the fair-comparison fix ran in the cheap direction. |
| `explain_all.py` | Harness that pulled `EXPLAIN` warnings per statement; how the upsert-key defect was found and confirmed fixed. |

## What still needs capturing

* **Confluent statement warnings in the console** — the `UPSERT_AND_PRIMARY_KEYS_DIFFERENT`
  and `HIGH_STATE_OPERATOR_WITHOUT_TTL` advisories. Worth a screenshot because
  the *point* is that Confluent surfaces them and MSF does not; the UI is the
  artefact. `EXPLAIN` output in `explain_all.py` is the text equivalent.
* **CloudWatch dashboard screenshot** — needs an authenticated console session.
* **Confluent drain results** — cap-10 and cap-20 rungs, not yet run.
