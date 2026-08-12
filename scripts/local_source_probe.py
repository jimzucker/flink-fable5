#!/usr/bin/env python3
"""
Instrument the SOURCE path: what the generator actually writes vs what the
Kafka source actually fetches, per partition.

Three environment hypotheses are already eliminated (CPU, disk, brokers), and
busy% sits at ~31% under every condition, so the pipeline is waiting for input.
The unexplained part is that inbound FALLS as requested load rises. This looks
at the two sides of that directly:

  produced  : Kafka end offsets per partition, delta over a window
  consumed  : Flink source numRecordsOut per subtask, delta over the same window
  lag       : produced - consumed, per partition

Skew matters here: if one partition holds most of the data, a source subtask
reading it becomes the limit no matter how much spare capacity exists elsewhere.

Usage: python3 scripts/local_source_probe.py [seconds]
"""
import json
import subprocess
import sys
import time
import urllib.request

B = "http://localhost:8081"
TOPICS = ["trades", "prices"]


def get(p):
    with urllib.request.urlopen(B + p, timeout=15) as r:
        return json.load(r)


def end_offsets(topic):
    """Kafka end offset per partition, via the broker container."""
    try:
        out = subprocess.run(
            # kafka-get-offsets.sh, NOT kafka-run-class GetOffsetShell --
            # the latter is absent in apache/kafka 3.8 and failed silently,
            # leaving the probe with only the consumed side.
            ["docker", "compose", "exec", "-T", "kafka",
             "/opt/kafka/bin/kafka-get-offsets.sh",
             "--bootstrap-server", "localhost:9092", "--topic", topic],
            capture_output=True, text=True, timeout=60).stdout
    except Exception as e:
        return {}
    res = {}
    for line in out.strip().split("\n"):
        parts = line.split(":")
        if len(parts) == 3:
            try:
                res[int(parts[1])] = int(parts[2])
            except ValueError:
                pass
    return res


def source_per_subtask():
    jobs = get("/jobs/overview").get("jobs", [])
    if not jobs:
        return {}
    jid = jobs[0]["jid"]
    res = {}
    for v in get(f"/jobs/{jid}")["vertices"]:
        if "Source" not in v.get("name", ""):
            continue
        which = "trades" if "trade" in v["name"].lower() else "prices"
        for st in range(v.get("parallelism", 1)):
            try:
                vals = get(f"/jobs/{jid}/vertices/{v['id']}/subtasks/{st}"
                           f"/metrics?get=numRecordsOut")
            except Exception:
                continue
            for m in vals:
                try:
                    res[(which, st)] = float(m.get("value") or 0)
                except (TypeError, ValueError):
                    pass
    return res


def main():
    secs = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    o0 = {t: end_offsets(t) for t in TOPICS}
    s0 = source_per_subtask()
    time.sleep(secs)
    o1 = {t: end_offsets(t) for t in TOPICS}
    s1 = source_per_subtask()

    print("SOURCE PROBE")
    for t in TOPICS:
        if not o0.get(t):
            print(f"  {t}: no offsets (broker query failed)")
            continue
        total = 0
        parts = []
        for p in sorted(o1[t]):
            d = o1[t][p] - o0[t].get(p, 0)
            total += d
            parts.append(f"p{p}={d/secs:,.0f}/s")
        print(f"  {t:7s} produced {total/secs:>9,.0f}/s   " + "  ".join(parts))
        if total:
            share = [(o1[t][p] - o0[t].get(p, 0)) / total for p in sorted(o1[t])]
            print(f"          partition share: " +
                  "  ".join(f"{x*100:.0f}%" for x in share))

    for which in ("trades", "prices"):
        subs = sorted(k[1] for k in s1 if k[0] == which)
        if not subs:
            continue
        tot = 0
        cells = []
        for st in subs:
            d = s1.get((which, st), 0) - s0.get((which, st), 0)
            tot += d
            cells.append(f"st{st}={d/secs:,.0f}/s")
        print(f"  {which:7s} consumed {tot/secs:>9,.0f}/s   " + "  ".join(cells))
    return 0


if __name__ == "__main__":
    sys.exit(main())
