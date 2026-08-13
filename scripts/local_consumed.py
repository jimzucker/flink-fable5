#!/usr/bin/env python3
"""
Trades/sec CONSUMED FROM KAFKA, from consumer-group committed offsets.

Flink's own numRecordsOut proved untrustworthy for this: on a single-edge
topology it read 29,970/s against a generator producing 10,667/s, and two
collectors reading the same metric disagreed (29,970 vs 0). Consumer-group
offsets are what the broker actually recorded being consumed.

Reports:
  consumed/s  - delta of CURRENT-OFFSET summed over partitions
  produced/s  - delta of LOG-END-OFFSET (independent ground truth)
  lag         - how far behind, and whether it is GROWING or SHRINKING

Lag direction is the real capacity signal: growing lag means the pipeline
cannot keep up at this rate, shrinking means it can.

Usage: python3 scripts/local_consumed.py [seconds] [topic]
"""
import subprocess
import sys
import time


def offsets(group, topic):
    try:
        out = subprocess.run(
            ["docker", "compose", "exec", "-T", "kafka",
             "/opt/kafka/bin/kafka-consumer-groups.sh",
             "--bootstrap-server", "localhost:9092",
             "--describe", "--group", group],
            capture_output=True, text=True, timeout=90).stdout
    except Exception as e:
        return None
    cur = end = 0
    for line in out.split("\n"):
        f = line.split()
        if len(f) >= 6 and f[1] == topic:
            try:
                cur += int(f[3])
                end += int(f[4])
            except ValueError:
                pass
    return cur, end


def groups():
    try:
        out = subprocess.run(
            ["docker", "compose", "exec", "-T", "kafka",
             "/opt/kafka/bin/kafka-consumer-groups.sh",
             "--bootstrap-server", "localhost:9092", "--list"],
            capture_output=True, text=True, timeout=90).stdout
    except Exception:
        return []
    return [g.strip() for g in out.split("\n") if g.strip()]


def main():
    secs = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    topic = sys.argv[2] if len(sys.argv) > 2 else "trades"
    gs = [g for g in groups() if topic in g or "positions" in g]
    if not gs:
        print(f"CONSUMED no consumer group found for {topic}")
        return 1
    g = gs[0]
    a = offsets(g, topic)
    if not a:
        print("CONSUMED could not read offsets")
        return 1
    time.sleep(secs)
    b = offsets(g, topic)
    consumed = (b[0] - a[0]) / secs
    produced = (b[1] - a[1]) / secs
    lag0, lag1 = a[1] - a[0], b[1] - b[0]
    trend = "GROWING" if lag1 > lag0 * 1.05 else (
        "shrinking" if lag1 < lag0 * 0.95 else "flat")
    print(f"CONSUMED {consumed:,.0f}/s  produced={produced:,.0f}/s  "
          f"lag {lag0:,} -> {lag1:,} ({trend})  group={g}")
    if trend == "GROWING":
        print("  -> cannot keep up at this rate; this IS the capacity limit")
    elif trend == "shrinking":
        print("  -> keeping up and draining backlog; not yet at capacity")
    else:
        print("  -> steady; consumed rate == produced rate")
    return 0


if __name__ == "__main__":
    sys.exit(main())
