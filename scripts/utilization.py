#!/usr/bin/env python3
"""
Flink and Kafka utilization as a percentage of capacity.

FLINK: mean busyTimeMsPerSecond across task operators, as a share of 1000ms/s.
This is what fraction of wall time the operators spend doing work rather than
waiting. Low utilization with high backpressure means the stage is blocked, not
idle -- so backpressure is reported alongside it.

KAFKA (MSK Serverless): BytesInPerSec against the documented per-cluster
ingress ceiling of 200 MB/s. Serverless publishes no capacity metric of its own,
so this is throughput against the service limit, not against provisioned
hardware. Egress is quoted against 400 MB/s.

Usage: python3 scripts/utilization.py [minutes]
"""
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

APP = "flink-fable5"
MSK_INGRESS_LIMIT_MBPS = 200.0
MSK_EGRESS_LIMIT_MBPS = 400.0


def query(ns, metric, dim_key, stat, minutes):
    expr = (f"SEARCH('{{{ns},{dim_key}}} MetricName=\"{metric}\"', '{stat}')")
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    r = subprocess.run(
        ["aws", "cloudwatch", "get-metric-data", "--metric-data-queries",
         json.dumps([{"Id": "q1", "Expression": expr, "Period": 60}]),
         "--start-time", start.strftime("%Y-%m-%dT%H:%M:%SZ"),
         "--end-time", end.strftime("%Y-%m-%dT%H:%M:%SZ"), "--output", "json"],
        capture_output=True, text=True)
    if r.returncode != 0:
        return []
    out = []
    for m in json.loads(r.stdout).get("MetricDataResults", []):
        vals = [v for v in (m.get("Values") or []) if v is not None]
        if vals:
            out.append((m.get("Label", ""), sum(vals) / len(vals)))
    return out


def main():
    minutes = int(sys.argv[1]) if len(sys.argv) > 1 else 8

    # Metrics persist after an app stops, so a stopped job still reports its
    # last busy values. Report the state or the number is read as current.
    st = subprocess.run(["aws", "kinesisanalyticsv2", "describe-application",
                         "--application-name", APP, "--query",
                         "ApplicationDetail.ApplicationStatus", "--output", "text"],
                        capture_output=True, text=True).stdout.strip()
    if st != "RUNNING":
        print(f"FLINK: application is {st or 'ABSENT'} — any busy%% below is STALE\n")

    busy = query("AWS/KinesisAnalytics", "busyTimeMsPerSecond",
                 "Application,Task", "Average", minutes)
    bp = query("AWS/KinesisAnalytics", "backPressuredTimeMsPerSecond",
               "Application,Task", "Average", minutes)

    print("FLINK")
    if busy:
        mean_busy = sum(v for _, v in busy) / len(busy) / 10.0     # ms/s -> %
        peak_busy = max(v for _, v in busy) / 10.0
        mean_bp = (sum(v for _, v in bp) / len(bp) / 10.0) if bp else 0.0
        print(f"  utilization : {mean_busy:5.1f}%  (peak operator {peak_busy:.1f}%)")
        print(f"  backpressure: {mean_bp:5.1f}%")
        # Verdict, not just a number. Idle-because-blocked and idle-because-
        # over-provisioned look identical in busy% and have opposite fixes:
        # one needs the bottleneck cleared, the other needs fewer KPUs.
        if mean_bp > 20 and mean_busy < 50:
            verdict = ("BLOCKED — idle but backpressured. NOT over-provisioned; "
                       "a downstream stage is the limit. Cutting KPUs will not help.")
        elif mean_busy >= 85:
            verdict = ("OVER-UTILIZED — saturated. More parallelism should convert "
                       "to throughput.")
        elif mean_busy < 40:
            verdict = (f"UNDER-UTILIZED — {100-mean_busy:.0f}% of paid compute idle. "
                       f"Reduce parallelism/KPU.")
        else:
            verdict = "OK — reasonably matched to load."
        print(f"  VERDICT     : {verdict}")
    else:
        print("  no operator data (job not running?)")

    bin_ = query("AWS/Kafka", "BytesInPerSec", "Cluster Name,Topic", "Average", minutes)
    bout = query("AWS/Kafka", "BytesOutPerSec", "Cluster Name,Topic", "Average", minutes)
    print("KAFKA (MSK Serverless)")
    if bin_ or bout:
        mb_in = sum(v for lbl, v in bin_ if APP in lbl or "flink-fable5" in lbl) / 1048576.0
        if mb_in == 0: mb_in = sum(v for _, v in bin_) / 1048576.0
        mb_out = sum(v for lbl, v in bout if APP in lbl or "flink-fable5" in lbl) / 1048576.0
        if mb_out == 0: mb_out = sum(v for _, v in bout) / 1048576.0
        print(f"  ingress     : {mb_in:7.2f} MB/s  = {mb_in/MSK_INGRESS_LIMIT_MBPS*100:5.2f}% of {MSK_INGRESS_LIMIT_MBPS:.0f} MB/s limit")
        print(f"  egress      : {mb_out:7.2f} MB/s  = {mb_out/MSK_EGRESS_LIMIT_MBPS*100:5.2f}% of {MSK_EGRESS_LIMIT_MBPS:.0f} MB/s limit")
        util = mb_in / MSK_INGRESS_LIMIT_MBPS * 100
        if util < 5:
            print(f"  VERDICT     : UNDER-UTILIZED on throughput ({util:.2f}% of ceiling) — "
                  "but serverless bills what you USE, so this is not waste by itself.")
        else:
            print(f"  VERDICT     : {util:.1f}% of ingress ceiling")
        # Partition-hours ARE waste when partitions exceed what consumers use.
        parts = int(subprocess.run(
            ["bash","-lc","cd infra && TF_WORKSPACE=fable5 terraform output -raw topics_partitions 2>/dev/null || echo 48"],
            capture_output=True, text=True).stdout.strip() or 48)
        topics = 7
        billed = parts * topics
        print(f"  partitions  : {parts}/topic x ~{topics} topics = ~{billed} billed partition-hours")
        print(f"                ~${billed*0.0015:.2f}/hr just to exist, before any data")
        print(f"  VERDICT     : output topics do not need {parts} partitions — only the")
        print(f"                two SOURCE topics need >= parallelism. Sizing outputs at 8")
        print(f"                would bill ~{2*parts+5*8} instead of ~{billed} partition-hours "
              f"(~${(billed-(2*parts+5*8))*0.0015:.2f}/hr saved).")
    else:
        print("  no cluster data")
    return 0


if __name__ == "__main__":
    sys.exit(main())
