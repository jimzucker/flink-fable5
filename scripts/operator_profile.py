#!/usr/bin/env python3
"""Per-operator bottleneck profile for an MSF job.

Whole-pipeline metrics cannot tell you WHICH stage refuses to parallelise. This
pulls MSF's TaskOperator-dimensioned metrics and reports, per operator:

  backpressure  the operator immediately UPSTREAM of a bottleneck backpressures,
                so the highest value points at the culprit's feeder
  busy          how saturated the operator is
  max vs mean   across subtasks. HIGH MAX + LOW MEAN IS SKEW -- one worker doing
                everyone's work, which a pipeline average hides completely.

usage: operator_profile.py [minutes]
"""
import json, subprocess, sys
from datetime import datetime, timedelta, timezone

APP = "flink-fable5"
NS = "AWS/KinesisAnalytics"
mins = int(sys.argv[1]) if len(sys.argv) > 1 else 10


def search(metric, stat):
    expr = (f"SEARCH('{{{NS},Application,Task,TaskOperator}} MetricName=\"{metric}\" "
            f"Application=\"{APP}\"', '{stat}')")
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=mins)
    q = [{"Id": "q1", "Expression": expr, "Period": 60}]
    r = subprocess.run(["aws", "cloudwatch", "get-metric-data",
                        "--metric-data-queries", json.dumps(q),
                        "--start-time", start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "--end-time", end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "--output", "json"], capture_output=True, text=True)
    if r.returncode != 0:
        return {}
    out = {}
    for s in json.loads(r.stdout).get("MetricDataResults", []):
        label = s.get("Label", "?")
        vals = [v for v in s.get("Values", []) if v is not None]
        if vals:
            out[label] = sum(vals) / len(vals)
    return out


bp_max = search("backPressuredTimeMsPerSecond", "Maximum")
busy_max = search("busyTimeMsPerSecond", "Maximum")
busy_avg = search("busyTimeMsPerSecond", "Average")

ops = sorted(set(bp_max) | set(busy_max), key=lambda k: -bp_max.get(k, 0))
if not ops:
    print("no per-operator data — job may not be running, or metrics level is not OPERATOR")
    sys.exit(1)

print(f"{'operator':46s} {'bp%':>6s} {'busy max%':>10s} {'busy avg%':>10s} {'skew':>7s}")
print("-" * 84)
for o in ops[:24]:
    bp = bp_max.get(o, 0) / 10
    bmx = busy_max.get(o, 0) / 10
    bav = busy_avg.get(o, 0) / 10
    skew = (bmx / bav) if bav > 0.5 else 0
    flag = "  <-- SKEW" if skew >= 3 else ("  <-- BOTTLENECK" if bp > 50 else "")
    name = o.split("/")[-1][:44]
    print(f"{name:46s} {bp:6.1f} {bmx:10.1f} {bav:10.1f} {skew:7.1f}{flag}")
print()
print("bp%      = backpressure. The operator upstream of the bottleneck shows this.")
print("skew     = busy max / busy avg across subtasks. >=3 means one worker is")
print("           carrying the stage while the others idle -- a hot key.")
