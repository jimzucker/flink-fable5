#!/usr/bin/env python3
"""
Phase 20 CORRECTED throughput: total records/sec, recomputed from raw
CloudWatch for a given time window.

WHY THIS EXISTS -- the bug it fixes:
phase20_rate.py read numRecordsOutPerSecond with stat "Average". MSF publishes
that metric once per SUBTASK under the same {Application,Task} dimensions, so
"Average" averages ACROSS SUBTASKS and yields a PER-SUBTASK rate, not a total.

That is the worst possible error for a scaling study: a per-subtask average is
normalized by parallelism, so under PERFECT linear scaling it stays flat, and
under real scaling it FALLS. Comparing P=20 against P=40 on it made near-linear
scaling (~1.95x) read as no scaling at all (0.98x), and produced a confident,
wrong "SQL doesn't scale".

total_rate = Average x subtask_count

subtask_count is DERIVED from the data (SampleCount / reports-per-period), not
assumed equal to job parallelism -- source tasks can run narrower than the job
when partitions < parallelism, and assuming would reintroduce the same class of
error one level down.

CALIBRATION: the generators emit exactly 4 x generator.trades.per.sec trades/s
(default 4 x 100 = 400/s), and the trades path is far too light to fall behind.
So the computed trades TOTAL must come out at ~400/s. That is an independent
ground truth on the arithmetic -- if it does not land there, this script is
wrong and its numbers must not be used. Every run prints the check.

Usage: phase20_totals.py <label> <start ISO8601Z> <end ISO8601Z> [expected_trades]
"""
import json
import re
import subprocess
import sys

NS = "AWS/KinesisAnalytics"
APP = "flink-fable5"


def datapoints(task, start, end):
    r = subprocess.run(
        ["aws", "cloudwatch", "get-metric-statistics", "--namespace", NS,
         "--metric-name", "numRecordsOutPerSecond",
         "--dimensions", f"Name=Application,Value={APP}", f"Name=Task,Value={task}",
         "--start-time", start, "--end-time", end,
         "--period", "60", "--statistics", "Average", "SampleCount",
         "--output", "json"],
        capture_output=True, text=True)
    if r.returncode != 0:
        return []
    return sorted(json.loads(r.stdout).get("Datapoints", []),
                  key=lambda x: x["Timestamp"])


def list_source_tasks(start, end):
    r = subprocess.run(
        ["aws", "cloudwatch", "list-metrics", "--namespace", NS,
         "--metric-name", "numRecordsOutPerSecond",
         "--dimensions", f"Name=Application,Value={APP}", "--output", "json"],
        capture_output=True, text=True)
    tasks = set()
    for m in json.loads(r.stdout).get("Metrics", []):
        d = {x["Name"]: x["Value"] for x in m["Dimensions"]}
        t = d.get("Task", "")
        if re.search(r"source", t, re.I):
            tasks.add(t)
    return sorted(tasks)


def main():
    label, start, end = sys.argv[1], sys.argv[2], sys.argv[3]
    expected_trades = float(sys.argv[4]) if len(sys.argv) > 4 else 400.0

    grand = 0.0
    trades_total = 0.0
    rows = []
    for task in list_source_tasks(start, end):
        pts = [p for p in datapoints(task, start, end) if p["SampleCount"] > 0]
        if not pts:
            continue
        avg = sum(p["Average"] for p in pts) / len(pts)
        # 4 reports per subtask per 60s bucket (MSF reports at 15s).
        subtasks = round(sum(p["SampleCount"] for p in pts) / len(pts) / 4)
        total = avg * subtasks
        rows.append((task, avg, subtasks, total))
        grand += total
        if re.search(r"trade", task, re.I):
            trades_total += total

    if not rows:
        print(f"TOTALS {label}: no source data in window")
        return 1

    print(f"TOTALS {label}  grand={grand:,.0f} rec/s")
    for task, avg, sub, total in rows:
        print(f"    {task[:52]:52s} avg={avg:>8,.1f}/subtask x {sub:>3d} "
              f"= {total:>10,.0f}/s")

    if trades_total:
        err = (trades_total - expected_trades) / expected_trades
        if abs(err) <= 0.20:
            print(f"  calibration: trades total {trades_total:,.0f}/s vs "
                  f"expected {expected_trades:,.0f}/s  OK -- multiplier confirmed, "
                  f"pipeline is KEEPING UP (steady state)")
        elif err > 0.20:
            # Only meaningful when the pipeline is caught up. Exceeding the
            # generator rate means it is draining a BACKLOG, which is expected
            # for a saturated run -- it does NOT indict the multiplier.
            print(f"  calibration: trades total {trades_total:,.0f}/s EXCEEDS the "
                  f"{expected_trades:,.0f}/s generated ({err*100:.0f}% over)")
            print("  -> pipeline is DRAINING BACKLOG, not in steady state. The "
                  "multiplier is fine;")
            print("     but this is drain throughput, so it depends on how much "
                  "backlog had piled up.")
            print("     Conditions run later in a chain face more backlog -- "
                  "compare with that in mind.")
        else:
            print(f"  calibration: trades total {trades_total:,.0f}/s is BELOW the "
                  f"{expected_trades:,.0f}/s generated ({-err*100:.0f}% under)")
            print("  -> falling behind on the trades path, or the multiplier is "
                  "wrong. Investigate before using.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
