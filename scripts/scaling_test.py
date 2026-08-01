#!/usr/bin/env python3
"""
Linear-scaling test: for each parallelism P, resubmit the job (config only,
no rebuild) and measure backlog catch-up throughput — a fresh job replays the
whole trades topic from earliest, so the sustained parse rate while lag > 0
is the pipeline's capacity at that parallelism.

Usage: python3 scripts/scaling_test.py [P ...]   (default: 1 2 4)
"""
import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request

PROM = "http://localhost:9090/api/v1/query"
FLINK = "http://localhost:8081"
CONFIG = "config/application.properties"


def prom(query):
    url = PROM + "?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=10) as resp:
        results = json.load(resp)["data"]["result"]
    return float(results[0]["value"][1]) if results else 0.0


def flink_json(path):
    with urllib.request.urlopen(FLINK + path, timeout=10) as resp:
        return json.load(resp)


def cancel_running_jobs():
    for job in flink_json("/jobs/overview")["jobs"]:
        if job["state"] == "RUNNING":
            req = urllib.request.Request(
                f"{FLINK}/jobs/{job['jid']}?mode=cancel", method="PATCH")
            urllib.request.urlopen(req, timeout=10).read()


def set_parallelism(p):
    with open(CONFIG) as f:
        content = f.read()
    content = re.sub(r"pipeline\.parallelism=\d+", f"pipeline.parallelism={p}", content)
    with open(CONFIG, "w") as f:
        f.write(content)


def run_case(p):
    print(f"\n--- parallelism={p}: resubmitting (config only) ---")
    set_parallelism(p)
    cancel_running_jobs()
    time.sleep(5)
    subprocess.run(["docker", "compose", "up", "-d", "--force-recreate", "job-submit"],
                   capture_output=True)
    for _ in range(40):
        time.sleep(3)
        jobs = [j for j in flink_json("/jobs/overview")["jobs"] if j["state"] == "RUNNING"]
        if jobs:
            break
    else:
        print("  job did not reach RUNNING"); return None
    time.sleep(15)  # warm-up

    # Capacity = backlog drain slope + live input rate. (The per-second meter
    # is a 60s window — it lags badly during a short catch-up, so measure the
    # backlog itself.)
    live_rate = prom('sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond{operator_name="parse_trade"})')
    observations = []
    for _ in range(12):
        t = time.time()
        pending = prom("sum(flink_taskmanager_job_task_operator_pendingRecords)")
        busy = prom("max(flink_taskmanager_job_task_busyTimeMsPerSecond)")
        observations.append((t, pending))
        print(f"    pending={pending:10.0f}  busy_max={busy:5.0f} ms/s")
        if pending == 0 and len(observations) > 2:
            break
        time.sleep(4)

    draining = [(t, pend) for t, pend in observations if pend > 0]
    if len(draining) >= 2:
        (t0, p0), (t1, p1) = draining[0], draining[-1]
        drain_slope = (p0 - p1) / (t1 - t0) if t1 > t0 else 0
        capacity = drain_slope + 1000  # generator keeps adding ~1000/s live
        print(f"  parallelism={p}: capacity ~ {capacity:.0f}/s "
              f"(drained {p0 - p1:.0f} backlog in {t1 - t0:.0f}s + live input)")
        return capacity
    print(f"  parallelism={p}: backlog drained too fast — capacity >> live rate ({live_rate:.0f}/s)")
    return None


def main():
    parallelisms = [int(a) for a in sys.argv[1:]] or [1, 2, 4]
    results = {}
    for p in parallelisms:
        results[p] = run_case(p)
    print("\n=== SCALING SUMMARY ===")
    base = None
    for p, cap in results.items():
        if cap:
            base = base or cap / p
            print(f"  P={p}: {cap:9.0f} rec/s   ({cap / results[parallelisms[0]]:.2f}x vs P={parallelisms[0]})")
        else:
            print(f"  P={p}: drained too fast to saturate")


if __name__ == "__main__":
    main()
