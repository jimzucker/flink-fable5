#!/usr/bin/env python3
"""
Sustained INBOUND and OUTBOUND processing rates, measured mid-drain.

WHY THIS EXISTS: sampling a job that has already consumed everything available
yields total/window, which is not a rate. Phase 23's backlog runs all reported
exactly backlog_size/30 -- identical across engines and partition counts, which
is how the artifact was spotted.

This measures counter deltas over a window and REFUSES to report unless the job
was still working at the end (lag remaining, or busy above a floor). A rate from
an idle job is not a rate.

  inbound  = records/s entering the pipeline at the sources
  outbound = records/s written to the four sink topics

Usage: python3 scripts/local_rates.py [seconds]
"""
import json
import sys
import time
import urllib.request

B = "http://localhost:8081"


def get(p):
    with urllib.request.urlopen(B + p, timeout=15) as r:
        return json.load(r)


def counters(jid):
    """Per-vertex numRecordsOut, split into sources and sinks."""
    inb = outb = 0.0
    for v in get(f"/jobs/{jid}")["vertices"]:
        name = v.get("name", "")
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
        if "Source" in name:
            inb += tot
        elif "Sink" in name or "sink" in name:
            outb += tot
    return inb, outb


def busy_now(jid):
    vals = []
    for v in get(f"/jobs/{jid}")["vertices"]:
        try:
            d = get(f"/jobs/{jid}/vertices/{v['id']}/backpressure")
        except Exception:
            continue
        if d.get("status") != "ok":
            continue
        for st in d.get("subtasks", []):
            if st.get("busyRatio") is not None:
                vals.append(st["busyRatio"])
    return (sum(vals) / len(vals) * 100) if vals else None


def main():
    secs = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    jobs = get("/jobs/overview").get("jobs", [])
    if not jobs:
        print("RATES no job running")
        return 1
    jid = jobs[0]["jid"]
    i0, o0 = counters(jid)
    time.sleep(secs)
    i1, o1 = counters(jid)
    busy = busy_now(jid)
    inb = (i1 - i0) / secs
    outb = (o1 - o0) / secs

    # Validity: if the job stopped consuming during the window it had run out of
    # work, and the "rate" is really total/window. Check it is still moving.
    time.sleep(10)
    i2, _ = counters(jid)
    still_working = (i2 - i1) / 10 > inb * 0.2

    par = max(v.get("parallelism", 0) for v in get(f"/jobs/{jid}")["vertices"])
    print(f"RATES inbound={inb:,.0f}/s outbound={outb:,.0f}/s "
          f"parallelism={par} busy={busy if busy is None else round(busy,1)}%")
    if not still_working:
        print("  INVALID: the job stopped consuming inside the window -- it ran "
              "out of backlog, so this is total/window, not a rate. Enlarge the "
              "backlog or shorten the sample.")
        return 2
    print("  valid: still consuming at sample end")
    return 0


if __name__ == "__main__":
    sys.exit(main())
