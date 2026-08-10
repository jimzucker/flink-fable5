#!/usr/bin/env python3
"""
Local consumer (source intake) COUNTER from the Flink REST API.

Reports the cumulative numRecordsOut counter, not the instantaneous
numRecordsOutPerSecond rate. A single sample of the per-second metric is noisy
and, taken right after a generator restart, catches the job mid-catch-up -- it
read 465/s on a load the pipeline demonstrably sustained at ~2,950/s. Sample
this counter twice and divide by elapsed time, exactly as the producer rate is
measured.

The metric is NAMESPACED by operator: "Source__trades-source.numRecordsOutPerSecond",
with a subtask-index prefix at vertex level ("0.Source__..."). Querying the bare
name "numRecordsOutPerSecond" returns an empty result, which reads as zero
throughput rather than as a failed query -- the same trap as the CloudWatch
dimension bug.

Sums numRecordsOutPerSecond across every subtask of every Source vertex.

Usage: python3 scripts/local_throughput.py
"""
import json
import urllib.request

B = "http://localhost:8081"


def get(p):
    with urllib.request.urlopen(B + p, timeout=15) as r:
        return json.load(r)


def main():
    jobs = get("/jobs/overview").get("jobs", [])
    if not jobs:
        print("CONSUMED no job running")
        return
    jid = jobs[0]["jid"]
    total = 0.0
    per_source = {}
    for v in get(f"/jobs/{jid}")["vertices"]:
        if "Source" not in v.get("name", ""):
            continue
        vt = 0.0
        for st in range(v.get("parallelism", 1)):
            try:
                ms = get(f"/jobs/{jid}/vertices/{v['id']}/subtasks/{st}/metrics")
            except Exception:
                continue
            # The metrics LIST endpoint returns names with value=None. Values
            # only come back when the name is requested via ?get=<name>.
            # Summing the list directly yields zero and reads as "no
            # throughput" rather than "wrong query".
            # Use the BARE task-level name. The operator-namespaced variants
            # (Source__x..., parse-x...) both exist inside a chained task, so
            # summing them double-counts: a 30 rec/s generator reported 60.
            want = ["numRecordsOut"]
            if not want:
                continue
            try:
                vals = get(f"/jobs/{jid}/vertices/{v['id']}/subtasks/{st}"
                           f"/metrics?get={','.join(want)}")
            except Exception:
                continue
            for m in vals:
                try:
                    vt += float(m.get("value") or 0)
                except (TypeError, ValueError):
                    pass
        per_source[v["name"][:38]] = vt
        total += vt
    print(f"COUNTER total={total:,.0f}")
    for k, val in per_source.items():
        print(f"    {k:40s} {val:>10,.0f}/s")


if __name__ == "__main__":
    main()
