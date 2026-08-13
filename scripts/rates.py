#!/usr/bin/env python3
"""
THE measurement. Three rates, one method, cumulative across ALL partitions.

  produced/s   - records written to the input topics      (end offsets)
  consumed/s   - records read by the job                  (consumer group offsets)
  published/s  - records written to the output topics     (end offsets)

All from Kafka, because Flink's counters proved untrustworthy: numRecordsOut
read 29,970/s on a single-edge topology while the generator produced 10,667/s,
and two collectors reading the same metric disagreed (29,970 vs 0). Kafka
offsets are what the broker actually recorded.

Every figure is a counter delta over the window, summed over every partition of
every topic. Lag direction says whether the job is keeping up.

Usage: python3 scripts/rates.py [seconds]
"""
import subprocess
import sys
import time

IN_TOPICS = ["trades", "prices"]
OUT_TOPICS = ["position-by-account-ticker", "position-by-ticker",
              "mv-by-account-ticker", "mv-by-ticker"]


def kafka(*args):
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "kafka"] + list(args),
        capture_output=True, text=True, timeout=120).stdout


def end_offsets(topic):
    """Sum of end offsets over ALL partitions. 0 if the topic does not exist."""
    out = kafka("/opt/kafka/bin/kafka-get-offsets.sh",
                "--bootstrap-server", "localhost:9092", "--topic", topic)
    tot = 0
    for line in out.strip().split("\n"):
        p = line.split(":")
        if len(p) == 3:
            try:
                tot += int(p[2])
            except ValueError:
                pass
    return tot


def group_offsets():
    """Sum of CURRENT and END offsets over ALL partitions of ALL groups."""
    cur = end = 0
    listing = kafka("/opt/kafka/bin/kafka-consumer-groups.sh",
                    "--bootstrap-server", "localhost:9092", "--list")
    for g in [x.strip() for x in listing.split("\n") if x.strip()]:
        out = kafka("/opt/kafka/bin/kafka-consumer-groups.sh",
                    "--bootstrap-server", "localhost:9092",
                    "--describe", "--group", g)
        for line in out.split("\n"):
            f = line.split()
            if len(f) >= 6 and f[1] in IN_TOPICS:
                try:
                    cur += int(f[3])
                    end += int(f[4])
                except ValueError:
                    pass
    return cur, end


def sample():
    produced = {t: end_offsets(t) for t in IN_TOPICS}
    published = {t: end_offsets(t) for t in OUT_TOPICS}
    cur, end = group_offsets()
    return produced, published, cur, end


def main():
    secs = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    p0, q0, c0, e0 = sample()
    time.sleep(secs)
    p1, q1, c1, e1 = sample()

    prod = sum(p1[t] - p0[t] for t in IN_TOPICS) / secs
    pub = sum(q1[t] - q0[t] for t in OUT_TOPICS) / secs
    cons = (c1 - c0) / secs
    lag0, lag1 = e0 - c0, e1 - c1
    trend = ("GROWING" if lag1 > lag0 * 1.05
             else "shrinking" if lag1 < lag0 * 0.95 else "flat")

    print(f"window {secs}s, cumulative over all partitions")
    print(f"  produced/s  {prod:>12,.0f}")
    print(f"  consumed/s  {cons:>12,.0f}")
    print(f"  published/s {pub:>12,.0f}")
    print(f"  lag         {lag0:>12,} -> {lag1:,}  ({trend})")
    print("")
    for t in IN_TOPICS:
        d = (p1[t] - p0[t]) / secs
        if d:
            print(f"    in  {t:32s} {d:>10,.0f}/s")
    for t in OUT_TOPICS:
        d = (q1[t] - q0[t]) / secs
        if d:
            print(f"    out {t:32s} {d:>10,.0f}/s")
    print("")
    if cons == 0 and prod > 0:
        print("  CONSUMED UNMEASURABLE -- no consumer-group offset progress.")
        print("  Flink commits offsets ON CHECKPOINT; with checkpointing disabled")
        print("  there is nothing to read. Enable checkpointing (a long interval")
        print("  is fine) or intake cannot be measured this way.")
        return 1
    if prod == 0 and cons == 0 and pub == 0:
        print("  NO DATA -- nothing produced, consumed or published in the window.")
        print("  Check the rig is fully up (kafka, taskmanager, generator, job).")
        return 1
    if trend == "GROWING":
        print("  consumed < produced: this IS the capacity limit")
    elif trend == "shrinking":
        print("  draining backlog: not yet at capacity")
    else:
        print("  steady: consumed == produced, capacity not reached")
    return 0


if __name__ == "__main__":
    sys.exit(main())
