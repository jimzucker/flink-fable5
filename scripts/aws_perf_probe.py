#!/usr/bin/env python3
"""
AWS performance probe: same measurements as perf_probe.py, sourced from
CloudWatch (namespace AWS/KinesisAnalytics, 1-min granularity).

Usage: python3 scripts/aws_perf_probe.py <label> [minutes]
"""
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

APP = "flink-demo"
NS = "AWS/KinesisAnalytics"

# (label, metric, TaskOperator filter or None for task-level, stat, aggregate-across-series)
SERIES = [
    ("trades_parsed_per_sec", "numRecordsOutPerSecond", "parse_trade", "Average", "sum"),
    ("prices_parsed_per_sec", "numRecordsOutPerSecond", "parse_price", "Average", "sum"),
    ("dedup_out_per_sec", "numRecordsOutPerSecond", "dedup_by_trade_id", "Average", "sum"),
    ("mv_account_out_per_sec", "numRecordsOutPerSecond", "mv_by_account_ticker", "Average", "sum"),
    ("busy_ms_per_sec_max", "busyTimeMsPerSecond", None, "Maximum", "max"),
    ("backpressure_ms_per_sec_max", "backPressuredTimeMsPerSecond", None, "Maximum", "max"),
    ("ticks_conflated_total", "demoTicksConflated", "mv_by_account_ticker", "Maximum", "sum"),
    ("kpus", "KPUs", None, "Maximum", "max"),
]


def search(metric, task_operator, stat, minutes):
    if metric == "KPUs":
        expr = (f"SEARCH('{{{NS},Application}} MetricName=\"{metric}\" "
                f"Application=\"{APP}\"', '{stat}')")
    elif task_operator:
        expr = (f"SEARCH('{{{NS},Application,Task,TaskOperator}} MetricName=\"{metric}\" "
                f"Application=\"{APP}\" TaskOperator=\"{task_operator}\"', '{stat}')")
    else:
        expr = (f"SEARCH('{{{NS},Application,Task}} MetricName=\"{metric}\" "
                f"Application=\"{APP}\"', '{stat}')")
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    query = [{"Id": "q1", "Expression": expr, "Period": 60}]
    result = subprocess.run(
        ["aws", "cloudwatch", "get-metric-data",
         "--metric-data-queries", json.dumps(query),
         "--start-time", start.strftime("%Y-%m-%dT%H:%M:%SZ"),
         "--end-time", end.strftime("%Y-%m-%dT%H:%M:%SZ"),
         "--output", "json"],
        capture_output=True, text=True)
    if result.returncode != 0:
        return []
    data = json.loads(result.stdout)
    # one result set per matched series; merge per-timestamp
    merged = {}
    for series in data.get("MetricDataResults", []):
        for ts, val in zip(series.get("Timestamps", []), series.get("Values", [])):
            merged.setdefault(ts, []).append(val)
    return merged


def main():
    label = sys.argv[1] if len(sys.argv) > 1 else "aws-probe"
    minutes = int(sys.argv[2]) if len(sys.argv) > 2 else 6
    # MSF emits per-subtask samples into one merged series: true rate = Average x parallelism
    parallelism = int(sys.argv[3]) if len(sys.argv) > 3 else 2

    print(f"\n=== AWS PERF PROBE: {label} (last {minutes} min, 1-min datapoints) ===")
    for name, metric, task_op, stat, agg in SERIES:
        merged = search(metric, task_op, stat, minutes)
        per_minute = []
        for ts in sorted(merged):
            vals = merged[ts]
            per_minute.append(sum(vals) if agg == "sum" else max(vals))
        if per_minute:
            scale = parallelism if name.endswith("per_sec") or name.startswith("ticks") else 1
            avg = sum(per_minute) / len(per_minute) * scale
            print(f"  {name:32s} avg={avg:12.1f}  max={max(per_minute) * scale:12.1f}  points={len(per_minute)}")
        else:
            print(f"  {name:32s} no data yet")


if __name__ == "__main__":
    main()
