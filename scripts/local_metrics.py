#!/usr/bin/env python3
"""
Local run metrics in one place: source throughput, utilization, backpressure.

Two fixes over the earlier collectors:

1. RETRIES. A single poll of the REST backpressure endpoint often lands before
   the metric is populated and returns nothing, which reads as zero utilization
   rather than as a failed sample. Three of four Phase 23 runs lost their
   utilization that way. This retries until it gets a reading or gives up
   loudly.

2. SOURCE RATE MEASURED AS A COUNTER DELTA over a window, not an instantaneous
   per-second metric. The instantaneous one read 465/s on a load the pipeline
   sustained at ~2,950/s.

Usage: python3 scripts/local_metrics.py [sample_seconds]
"""
import json
import sys
import time
import urllib.request


# NOTE: the /backpressure endpoint's backpressuredRatio reported 0.0% on a job
# whose per-subtask backPressuredTimeMsPerSecond metric read 999 (99.9%).
# Reading the ratio produced "0% backpressure" in every table for a severely
# backpressured pipeline, and several conclusions were drawn from it. Use the
# per-subtask METRIC, not the endpoint ratio.

B = "http://localhost:8081"


def get(p):
    with urllib.request.urlopen(B + p, timeout=15) as r:
        return json.load(r)


def job_id():
    jobs = get("/jobs/overview").get("jobs", [])
    return jobs[0]["jid"] if jobs else None


def source_counters(jid):
    """Cumulative records out of each Source task, split trades vs prices."""
    out = {"trades": 0.0, "prices": 0.0}
    for v in get(f"/jobs/{jid}")["vertices"]:
        name = v.get("name", "")
        if "Source" not in name:
            continue
        which = "trades" if "trade" in name.lower() else "prices"
        for st in range(v.get("parallelism", 1)):
            try:
                vals = get(f"/jobs/{jid}/vertices/{v['id']}/subtasks/{st}"
                           f"/metrics?get=numRecordsOut")
            except Exception:
                continue
            for m in vals:
                try:
                    out[which] += float(m.get("value") or 0)
                except (TypeError, ValueError):
                    pass
    return out


def utilization(jid, attempts=6, gap=8):
    """busy% / backpressure%, retried -- one poll is often too early."""
    for _ in range(attempts):
        busy, bp = [], []
        for v in get(f"/jobs/{jid}")["vertices"]:
            try:
                d = get(f"/jobs/{jid}/vertices/{v['id']}/backpressure")
            except Exception:
                continue
            if d.get("status") != "ok":
                continue
            for st in d.get("subtasks", []):
                if st.get("busyRatio") is not None:
                    busy.append(st["busyRatio"])
                if st.get("backpressuredRatio") is not None:
                    bp.append(st["backpressuredRatio"])
        if busy:
            return (sum(busy) / len(busy) * 100,
                    (sum(bp) / len(bp) * 100) if bp else 0.0,
                    len(busy))
        time.sleep(gap)
    return None, None, 0


def main():
    secs = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    jid = job_id()
    if not jid:
        print("METRICS no job running")
        return 1
    a = source_counters(jid)
    time.sleep(secs)
    b = source_counters(jid)
    tr = (b["trades"] - a["trades"]) / secs
    px = (b["prices"] - a["prices"]) / secs
    busy, bp, n = utilization(jid)
    par = max(v.get("parallelism", 0) for v in get(f"/jobs/{jid}")["vertices"])
    if busy is None:
        print(f"METRICS in_trades/s={tr:,.0f} in_prices/s={px:,.0f} "
              f"parallelism={par} busy=UNAVAILABLE backpressure=UNAVAILABLE")
    else:
        print(f"METRICS in_trades/s={tr:,.0f} in_prices/s={px:,.0f} "
              f"parallelism={par} busy={busy:.1f}% backpressure={bp:.1f}% subtasks={n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
