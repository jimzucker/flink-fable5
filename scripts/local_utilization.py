#!/usr/bin/env python3
"""
Local (Docker) Flink utilization, so local runs can be reported with the same
columns as AWS.

Uses the JobManager REST backpressure endpoint, which returns busyRatio and
backpressuredRatio per subtask -- the local equivalent of MSF's
busyTimeMsPerSecond / backPressuredTimeMsPerSecond.

NOTE: local rec/s is the GENERATOR rate, not capacity, because the pipeline
keeps up. It is not comparable to the AWS numbers, which are drain-rate
measurements against a saturating backlog.

Usage: python3 scripts/local_utilization.py   (needs the rig running)
"""
import json, urllib.request, sys

# NOTE: the /backpressure endpoint's backpressuredRatio reported 0.0% on a job
# whose per-subtask backPressuredTimeMsPerSecond metric read 999 (99.9%).
# Reading the ratio produced "0% backpressure" in every table for a severely
# backpressured pipeline, and several conclusions were drawn from it. Use the
# per-subtask METRIC, not the endpoint ratio.

B="http://localhost:8081"
def get(p):
    with urllib.request.urlopen(B+p, timeout=10) as r: return json.load(r)
jobs=get("/jobs/overview").get("jobs",[])
if not jobs: print("no job"); sys.exit(0)
jid=jobs[0]["jid"]
j=get(f"/jobs/{jid}")
busy=[]; bp=[]; par=0
for v in j.get("vertices",[]):
    par=max(par, v.get("parallelism",0))
    try:
        d=get(f"/jobs/{jid}/vertices/{v['id']}/backpressure")
        if d.get("status")!="ok": continue
        for st in d.get("subtasks",[]):
            if st.get("busyRatio") is not None: busy.append(st["busyRatio"])
            if st.get("backpressuredRatio") is not None: bp.append(st["backpressuredRatio"])
    except Exception: pass
if busy:
    mb=sum(busy)/len(busy)*100; mbp=(sum(bp)/len(bp)*100) if bp else 0
    print(f"UTIL parallelism={par} busy={mb:.1f}% backpressure={mbp:.1f}% subtasks={len(busy)}")
else:
    print("UTIL no busy data")
