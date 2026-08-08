#!/usr/bin/env python3
"""
Phase 20 throughput sampler -- ONE metric definition, used identically for
every condition (SQL/DataStream x P=20/P=40 x materializer on/off).

WHY SOURCE-SIDE: the fair unit is records the pipeline INGESTED, not records it
emitted. Flink SQL on an updating stream emits a retract + an insert for every
change, so counting output records inflates SQL roughly 2x against DataStream
for doing the same work. Source intake is defined identically for both.

Under saturating load the sources are backpressured (measured: prices source at
98.8% bp), so their intake rate IS the pipeline's capacity -- exactly what we
want to compare.

Dimension set is {Application,Task}. NOT TaskOperator -- that dimension does not
exist on these metrics, and CloudWatch SEARCH returns an empty result for an
unmatched dimension set rather than an error, which reads exactly like "the job
isn't running". That fault silently emptied three earlier profiling runs.

Usage: python3 scripts/phase20_rate.py <label> [minutes]
"""
import json
import os
import re
import statistics
import subprocess
import sys
from datetime import datetime, timedelta, timezone

APP = os.environ.get("FLINK_APP", "flink-fable5")
NS = "AWS/KinesisAnalytics"


def get_series(metric, stat, minutes):
    """Return {task_name: [values...]} for a task-level metric."""
    expr = (f"SEARCH('{{{NS},Application,Task}} MetricName=\"{metric}\" "
            f"Application=\"{APP}\"', '{stat}')")
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    query = [{"Id": "q1", "Expression": expr, "Period": 60}]
    r = subprocess.run(
        ["aws", "cloudwatch", "get-metric-data",
         "--metric-data-queries", json.dumps(query),
         "--start-time", start.strftime("%Y-%m-%dT%H:%M:%SZ"),
         "--end-time", end.strftime("%Y-%m-%dT%H:%M:%SZ"),
         "--output", "json"],
        capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  cloudwatch error: {r.stderr.strip()[:300]}", file=sys.stderr)
        return {}
    out = {}
    for m in json.loads(r.stdout).get("MetricDataResults", []):
        label = m.get("Label", "")
        vals = [v for v in m.get("Values", []) if v is not None]
        if vals:
            out.setdefault(label, []).extend(vals)
    return out


def is_source(task_name):
    return bool(re.search(r"source", task_name, re.I))


def main():
    label = sys.argv[1] if len(sys.argv) > 1 else "run"
    minutes = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    # Source tasks have no upstream, so numRecordsIn is a flat zero on them --
    # the records they pull off Kafka show up as numRecordsOut. Both APIs fuse
    # exactly one output per input record into the source chain (SQL:
    # src->Calc->WindowTableFunction, DataStream: src->parse), so this counts
    # the same thing on both sides.
    series = get_series("numRecordsOutPerSecond", "Average", minutes)
    src = {k: v for k, v in series.items() if is_source(k)}
    if not src:
        print(f"RATE {label}: no source-task data "
              f"(tasks seen: {list(series)[:5]})")
        return 1

    # Per minute-bucket total across source tasks: line up by index, since all
    # series share the same 60s period and window.
    n = min(len(v) for v in src.values())
    if n == 0:
        print(f"RATE {label}: source tasks present but no samples")
        return 1
    totals = [sum(v[i] for v in src.values()) for i in range(n)]
    totals.sort()

    med = statistics.median(totals)
    p90 = totals[max(0, int(0.9 * len(totals)) - 1)]
    print(f"RATE {label}  n={len(totals)}  median={med:,.0f}/s  "
          f"p90={p90:,.0f}/s  max={max(totals):,.0f}/s")
    print(f"  source tasks: {len(src)}")
    for k in sorted(src):
        print(f"    {k[:60]:60s} mean={statistics.mean(src[k]):>10,.0f}/s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
