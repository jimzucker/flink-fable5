#!/usr/bin/env python3
"""
Confluent utilization, reported the SAME way as scripts/utilization.py on AWS.

The point is comparability. AWS reports Flink busy% and Kafka throughput against
a ceiling; this reports the Confluent equivalents with the same UNDER/OVER
verdicts, so the two platforms can sit in one table without mixing methods --
which is what invalidated the earlier cross-platform numbers.

  AWS                          Confluent
  ---------------------------  ------------------------------------------
  Flink busyTimeMsPerSecond    current_cfus / max_cfu (compute pool draw)
  backPressuredTimeMsPerSecond (no direct equivalent; use lag trend)
  MSK BytesInPerSec / limit    received_bytes, sent_bytes
  partition-hours billed       partition_count
  (n/a)                        cluster_load_percent  <- eCKU saturation

cluster_load_percent is the metric Confluent itself says to scale on: sustained
>70% means add eCKUs. It has no AWS analogue -- MSK Serverless exposes no
capacity percentage at all -- so it is reported as Confluent-only rather than
forced into a false pairing.

Usage: CONFLUENT_CLOUD_API_KEY / _SECRET in env or ~/.confluent-creds
       python3 scripts/confluent_utilization.py <cluster-id> [pool-id] [minutes]
"""
import base64
import json
import pathlib
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

API = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"


def creds():
    import os
    k = os.environ.get("CONFLUENT_CLOUD_API_KEY")
    s = os.environ.get("CONFLUENT_CLOUD_API_SECRET")
    p = pathlib.Path.home() / ".confluent-creds"
    if (not k) and p.exists():
        for line in p.read_text().splitlines():
            line = line.strip()
            if line.startswith("CONFLUENT_CLOUD_API_KEY"):
                k = line.split("=", 1)[1].strip().strip('"')
            if line.startswith("CONFLUENT_CLOUD_API_SECRET"):
                s = line.split("=", 1)[1].strip().strip('"')
    if not k or not s:
        sys.exit("set CONFLUENT_CLOUD_API_KEY / _SECRET, or create ~/.confluent-creds")
    return k, s


def q(metric, filt, minutes, agg="AVG"):
    k, s = creds()
    auth = base64.b64encode(f"{k}:{s}".encode()).decode()
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = end - timedelta(minutes=minutes)
    body = {
        "aggregations": [{"metric": metric, "agg": agg}],
        "filter": filt,
        "granularity": "PT1M",
        "intervals": [f"{start.isoformat().replace('+00:00','Z')}/"
                      f"{end.isoformat().replace('+00:00','Z')}"],
        "limit": 1000,
    }
    req = urllib.request.Request(
        API, data=json.dumps(body).encode(),
        headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.load(r).get("data", [])
    except Exception as e:
        print(f"  ({metric}: query failed — {str(e)[:90]})")
        return []
    return [p.get("value") for p in data if p.get("value") is not None]


def field(cluster_id):
    return {"field": "resource.kafka.id", "op": "EQ", "value": cluster_id}


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    cluster = sys.argv[1]
    pool = sys.argv[2] if len(sys.argv) > 2 else None
    minutes = int(sys.argv[3]) if len(sys.argv) > 3 else 10

    print("FLINK (Confluent compute pool)")
    if pool:
        cfu = q("io.confluent.flink/compute_pool_utilization/current_cfus",
                {"field": "resource.compute_pool.id", "op": "EQ", "value": pool}, minutes)
        if cfu:
            cur, peak = sum(cfu) / len(cfu), max(cfu)
            print(f"  CFU drawn   : {cur:.2f} avg, {peak:.2f} peak")
            print("  VERDICT     : compare against the pool's max_cfu. Drawing well")
            print("                below the cap with a backlog present is the")
            print("                autoscaler declining capacity, NOT a busy pool.")
        else:
            print("  no CFU data")
    else:
        print("  (pass a compute-pool id to measure CFU draw)")

    print("KAFKA (Confluent cluster)")
    load = q("io.confluent.kafka.server/cluster_load_percent", field(cluster), minutes)
    if load:
        avg, peak = sum(load) / len(load) * 100, max(load) * 100
        if avg >= 70:
            v = "OVER-UTILIZED — Confluent's own guidance is to add eCKUs above 70%"
        elif avg < 20:
            v = f"UNDER-UTILIZED — {avg:.1f}% load; eCKUs are being paid for and not used"
        else:
            v = "OK"
        print(f"  cluster load: {avg:5.1f}% avg, {peak:5.1f}% peak")
        print(f"  VERDICT     : {v}")
    else:
        print("  no cluster_load_percent (Basic clusters do not publish it)")

    rx = q("io.confluent.kafka.server/received_bytes", field(cluster), minutes, "SUM")
    tx = q("io.confluent.kafka.server/sent_bytes", field(cluster), minutes, "SUM")
    if rx:
        print(f"  ingress     : {sum(rx)/len(rx)/1048576:7.2f} MB/s")
    if tx:
        print(f"  egress      : {sum(tx)/len(tx)/1048576:7.2f} MB/s")
    parts = q("io.confluent.kafka.server/partition_count", field(cluster), minutes, "MAX")
    if parts:
        p = max(parts)
        print(f"  partitions  : {p:.0f}")
        print("  VERDICT     : as on AWS, only SOURCE topics need partitions >=")
        print("                parallelism; output topics sized to match are waste.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
