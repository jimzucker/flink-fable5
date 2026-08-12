#!/usr/bin/env python3
"""
Backpressure per vertex, from the per-subtask METRIC.

The /backpressure endpoint's backpressuredRatio reported 0.0% on a job whose
backPressuredTimeMsPerSecond metric read 999 -- 99.9% blocked. Every "0%
backpressure" in this project's tables came from that endpoint, and the wrong
conclusions ("not backpressured, just starved", "source-bound") followed from it.

Reads busy / idle / backpressure per vertex and names the likely bottleneck:
the operator with HIGH busy and LOW backpressure. Everything upstream of it
shows high backpressure.

Usage: python3 scripts/local_backpressure.py
"""
import json
import urllib.request

B = "http://localhost:8081"
WANT = ["busyTimeMsPerSecond", "idleTimeMsPerSecond",
        "backPressuredTimeMsPerSecond"]


def get(p):
    with urllib.request.urlopen(B + p, timeout=15) as r:
        return json.load(r)


def main():
    jobs = get("/jobs/overview").get("jobs", [])
    if not jobs:
        print("no job running")
        return 1
    jid = jobs[0]["jid"]
    rows = []
    for v in get(f"/jobs/{jid}")["vertices"]:
        par = v.get("parallelism", 1)
        acc = {k: [] for k in WANT}
        for st in range(par):
            try:
                vals = get(f"/jobs/{jid}/vertices/{v['id']}/subtasks/{st}"
                           f"/metrics?get={','.join(WANT)}")
            except Exception:
                continue
            for m in vals:
                try:
                    acc[m["id"]].append(float(m.get("value") or 0))
                except (TypeError, ValueError, KeyError):
                    pass
        if not acc["busyTimeMsPerSecond"]:
            continue
        avg = {k: (sum(x) / len(x) / 10 if x else 0.0) for k, x in acc.items()}
        rows.append((v["name"], par, avg["busyTimeMsPerSecond"],
                     avg["idleTimeMsPerSecond"], avg["backPressuredTimeMsPerSecond"]))

    print(f"{'operator':44s} {'par':>3} {'busy%':>7} {'idle%':>7} {'bp%':>7}")
    for name, par, busy, idle, bp in rows:
        print(f"{name[:44]:44s} {par:>3} {busy:>6.1f}% {idle:>6.1f}% {bp:>6.1f}%")

    # bottleneck: does real work AND is not itself blocked
    cands = [r for r in rows if r[2] > 20 and r[4] < 50]
    print("")
    if cands:
        cands.sort(key=lambda r: -r[2])
        n, par, busy, idle, bp = cands[0]
        print(f"LIKELY BOTTLENECK: {n}")
        print(f"  busy {busy:.1f}%  backpressure {bp:.1f}%  parallelism {par}")
        print("  -- it works while everything upstream waits on it")
    else:
        print("No operator shows high busy with low backpressure.")
        print("Either nothing is saturated, or the sink/external system is the limit.")
    return 0


if __name__ == "__main__":
    main()
