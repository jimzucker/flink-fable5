#!/usr/bin/env python3
"""
Throughput probe for the Confluent Cloud stack, via the Confluent Metrics API
(the counterpart of aws_perf_probe.py / CloudWatch).

Reports per-topic produced (received_records) and consumed (sent_records)
rates in msgs/s over the last N minutes — enough to see the generator's input
rate, the fan-in the statements consume, and the conflated/output rates.

Usage:
  export CONFLUENT_CLOUD_API_KEY=... CONFLUENT_CLOUD_API_SECRET=...   # Cloud key (same as Terraform)
  python3 scripts/confluent_perf_probe.py [--minutes 10]
Cluster id comes from `terraform output` in infra-confluent/ (override with
--cluster-id lkc-xxxx).

Stdlib only — no pip installs needed.
"""
import argparse
import base64
import json
import os
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

METRICS_URL = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def cluster_id_from_terraform():
    result = subprocess.run(
        ["terraform", "-chdir=" + os.path.join(REPO_ROOT, "infra-confluent"),
         "output", "-raw", "kafka_cluster_id"],
        capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit("could not read kafka_cluster_id from terraform output; pass --cluster-id lkc-xxxx")
    return result.stdout.strip()


def query(auth, cluster_id, metric, minutes):
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    body = {
        "aggregations": [{"metric": f"io.confluent.kafka.server/{metric}"}],
        "filter": {"field": "resource.kafka.id", "op": "EQ", "value": cluster_id},
        "granularity": "PT1M",
        "group_by": ["metric.topic"],
        "intervals": [f"{(now - timedelta(minutes=minutes)).isoformat()}/{now.isoformat()}"],
        "limit": 1000,
    }
    req = urllib.request.Request(
        METRICS_URL, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Basic {auth}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["data"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=int, default=10)
    parser.add_argument("--cluster-id", default=None)
    args = parser.parse_args()

    key = os.environ.get("CONFLUENT_CLOUD_API_KEY")
    secret = os.environ.get("CONFLUENT_CLOUD_API_SECRET")
    if not (key and secret):
        sys.exit("set CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET (Cloud API key)")
    auth = base64.b64encode(f"{key}:{secret}".encode()).decode()
    cluster_id = args.cluster_id or cluster_id_from_terraform()

    print(f"cluster {cluster_id}, last {args.minutes} min (msgs/s, per-minute datapoints averaged)")
    for metric, label in [("received_records", "produced"), ("sent_records", "consumed")]:
        data = query(auth, cluster_id, metric, args.minutes)
        by_topic = {}
        for point in data:
            by_topic.setdefault(point["metric.topic"], []).append(point["value"])
        print(f"\n  {label} ({metric}):")
        if not by_topic:
            print("    (no datapoints — metrics lag ~2-5 min behind real time)")
        for topic in sorted(by_topic):
            values = by_topic[topic]
            avg = sum(values) / len(values) / 60.0
            peak = max(values) / 60.0
            print(f"    {topic:32s} avg {avg:10.1f}/s   peak {peak:10.1f}/s   ({len(values)} pts)")


if __name__ == "__main__":
    main()
