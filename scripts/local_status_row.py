#!/usr/bin/env python3
"""
One full status row: in trades/s, in prices/s, out positions/s, out MV/s,
parallelism, utilization%, backpressure%.

Rates are counter deltas over a window. Backpressure comes from the per-subtask
backPressuredTimeMsPerSecond METRIC, not the /backpressure endpoint -- that
endpoint reported 0.0% on a job that was 99.9% blocked, and every backpressure
figure in this project's earlier tables came from it.

Usage: python3 scripts/local_status_row.py [seconds]
"""
import json
import sys
import time
import urllib.request

B = "http://localhost:8081"


def get(p):
    with urllib.request.urlopen(B + p, timeout=15) as r:
        return json.load(r)


def out_offsets():
    """Published records from KAFKA end offsets, not Flink vertex names.

    Vertex names differ by engine -- DataStream has "sink-mv-account-ticker"
    while SQL has a bare "Sink: Writer" -- so name matching silently returned
    zero for SQL. Offsets are engine-independent and are what a consumer
    actually sees.
    """
    import subprocess
    tot = {"out_pos": 0, "out_mv": 0}
    topics = {"position-by-account-ticker": "out_pos",
              "position-by-ticker": "out_pos",
              "mv-by-account-ticker": "out_mv",
              "mv-by-ticker": "out_mv"}
    for t, bucket in topics.items():
        try:
            out = subprocess.run(
                ["docker", "compose", "exec", "-T", "kafka",
                 "/opt/kafka/bin/kafka-get-offsets.sh",
                 "--bootstrap-server", "localhost:9092", "--topic", t],
                capture_output=True, text=True, timeout=60).stdout
        except Exception:
            continue
        for line in out.strip().split("\n"):
            p = line.split(":")
            if len(p) == 3:
                try:
                    tot[bucket] += int(p[2])
                except ValueError:
                    pass
    return tot


def snapshot(jid):
    """Source intake from Flink counters; published totals from Kafka offsets."""
    b = {"in_trades": 0.0, "in_prices": 0.0}
    for v in get(f"/jobs/{jid}")["vertices"]:
        name = v.get("name", "").lower()
        if "source" not in name:
            continue
        tot = 0.0
        for st in range(v.get("parallelism", 1)):
            try:
                vals = get(f"/jobs/{jid}/vertices/{v['id']}/subtasks/{st}"
                           f"/metrics?get=numRecordsOut")
            except Exception:
                continue
            for m in vals:
                try:
                    tot += float(m.get("value") or 0)
                except (TypeError, ValueError):
                    pass
        if "trade" in name:
            b["in_trades"] += tot
        elif "price" in name:
            b["in_prices"] += tot
    b.update(out_offsets())
    return b


def util(jid):
    busy, bp = [], []
    for v in get(f"/jobs/{jid}")["vertices"]:
        for st in range(v.get("parallelism", 1)):
            try:
                vals = get(f"/jobs/{jid}/vertices/{v['id']}/subtasks/{st}"
                           f"/metrics?get=busyTimeMsPerSecond,"
                           f"backPressuredTimeMsPerSecond")
            except Exception:
                continue
            for m in vals:
                try:
                    x = float(m.get("value") or 0)
                except (TypeError, ValueError):
                    continue
                (busy if m["id"] == "busyTimeMsPerSecond" else bp).append(x)
    return ((sum(busy) / len(busy) / 10) if busy else None,
            (sum(bp) / len(bp) / 10) if bp else None)


def main():
    secs = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    jobs = get("/jobs/overview").get("jobs", [])
    if not jobs:
        print("ROW no job")
        return 1
    jid = jobs[0]["jid"]
    a = snapshot(jid)
    time.sleep(secs)
    b = snapshot(jid)
    r = {k: (b[k] - a[k]) / secs for k in a}
    busy, bp = util(jid)
    par = max(v.get("parallelism", 0) for v in get(f"/jobs/{jid}")["vertices"])
    print(f"ROW in_trades={r['in_trades']:,.0f} in_prices={r['in_prices']:,.0f} "
          f"out_pos={r['out_pos']:,.0f} out_mv={r['out_mv']:,.0f} "
          f"par={par} busy={'?' if busy is None else round(busy,1)} "
          f"bp={'?' if bp is None else round(bp,1)}")
    return 0


if __name__ == "__main__":
    main()
