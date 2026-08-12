#!/usr/bin/env python3
"""
Trace source subtasks over time to catch one stalling.

Aggregate rates hid this: at 20k requested, prices consumed 1,257/s with
st0=1,257 and st1=0 -- one subtask doing everything, the other nothing, while
partitions were perfectly even (24/23/26/27%). So it is not data skew; a subtask
stopped fetching.

Samples per subtask every few seconds and reports:
  records/s      - is it moving at all
  idleTimeMs/s   - is it waiting for input (starved)
  busyTimeMs/s   - is it working
  backPressuredTimeMs/s - is it blocked downstream

Those three separate "stalled because nothing to read" from "stalled because
blocked" from "stalled for another reason" -- which aggregate throughput cannot.

Usage: python3 scripts/local_subtask_trace.py [samples] [gap_seconds]
"""
import json
import sys
import time
import urllib.request

B = "http://localhost:8081"
WANT = ["numRecordsOut", "idleTimeMsPerSecond", "busyTimeMsPerSecond",
        "backPressuredTimeMsPerSecond"]


def get(p):
    with urllib.request.urlopen(B + p, timeout=15) as r:
        return json.load(r)


def sources(jid, only_sources=False):
    """All vertices by default. The bottleneck is the operator with HIGH busy
    and LOW backpressure -- everything upstream of it shows high backpressure.
    Filtering to sources only shows the symptom, never the cause."""
    out = []
    for v in get(f"/jobs/{jid}")["vertices"]:
        if only_sources and "Source" not in v.get("name", ""):
            continue
        out.append((v["id"], v["name"], v.get("parallelism", 1)))
    return out


def sample(jid, vid, st):
    try:
        vals = get(f"/jobs/{jid}/vertices/{vid}/subtasks/{st}"
                   f"/metrics?get={','.join(WANT)}")
    except Exception:
        return {}
    d = {}
    for m in vals:
        try:
            d[m["id"]] = float(m.get("value") or 0)
        except (TypeError, ValueError):
            pass
    return d


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    gap = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    jobs = get("/jobs/overview").get("jobs", [])
    if not jobs:
        print("TRACE no job running")
        return 1
    jid = jobs[0]["jid"]
    srcs = sources(jid)
    prev = {}
    print(f"TRACE {n} samples, {gap}s apart")
    print(f"{'t':>4}  {'operator':30s} {'st':>3} {'rec/s':>9} "
          f"{'idle%':>7} {'busy%':>7} {'bp%':>6}")
    for i in range(n):
        for vid, name, par in srcs:
            for st in range(min(par, 1)):
                d = sample(jid, vid, st)
                if not d:
                    continue
                key = (name, st)
                cur = d.get("numRecordsOut", 0)
                rate = ""
                if key in prev:
                    rate = f"{(cur - prev[key]) / gap:,.0f}"
                prev[key] = cur
                short = name[:30]
                print(f"{i*gap:>4}  {short:30s} {st:>3} {rate:>9} "
                      f"{d.get('idleTimeMsPerSecond',0)/10:>6.1f}% "
                      f"{d.get('busyTimeMsPerSecond',0)/10:>6.1f}% "
                      f"{d.get('backPressuredTimeMsPerSecond',0)/10:>5.1f}%")
        print("")
        time.sleep(gap)
    return 0


if __name__ == "__main__":
    main()
