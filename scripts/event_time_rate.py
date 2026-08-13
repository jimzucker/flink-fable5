#!/usr/bin/env python3
"""
Throughput and latency from EVENT TIME carried source -> sink.

Every measurement path tried so far has failed in a different way:
  Flink numRecordsOut   - read 29,970/s against a 10,667/s generator, and two
                          collectors disagreed (29,970 vs 0)
  consumer-group offsets- only commit ON CHECKPOINT, so with checkpointing off
                          consumed and lag both read 0
  /backpressure endpoint- reported 0.0% on a job that was 99.9% blocked

The output records carry `as_of` = the SOURCE event time of the newest trade
folded into that value. That is data, not instrumentation, so it cannot be
disabled, mis-scoped, or mis-aggregated.

  event-time progress = how far as_of advances per second of wall clock
      ratio ~1.0  -> keeping up: one second of source time per second of real
      ratio <1.0  -> falling behind by exactly that factor
      ratio >1.0  -> catching up, replaying backlog faster than real time

  end-to-end latency  = wall clock now - newest as_of

Usage: python3 scripts/event_time_rate.py [seconds] [topic]
"""
import json
import subprocess
import sys
import time

DEFAULT_TOPIC = "position-by-ticker"


def newest_as_of(topic, max_records=800):
    """Max as_of across the tail of every partition."""
    try:
        out = subprocess.run(
            ["docker", "compose", "exec", "-T", "kafka",
             "/opt/kafka/bin/kafka-console-consumer.sh",
             "--bootstrap-server", "localhost:9092",
             # NO --from-beginning: that re-reads the FIRST records every
             # sample, so as_of never advances and the pipeline looks stalled
             # when it is fine. Consume newly-arriving records instead.
             "--topic", topic,
             "--max-messages", str(max_records),
             "--timeout-ms", "6000"],
            capture_output=True, text=True, timeout=60).stdout
    except Exception:
        return None
    best = None
    for line in out.split("\n"):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            v = json.loads(line).get("as_of")
            if v and (best is None or v > best):
                best = int(v)
        except Exception:
            pass
    return best


def main():
    secs = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    topic = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_TOPIC

    t0 = time.time()
    a = newest_as_of(topic)
    if a is None:
        print("EVENT-TIME no as_of found -- topic empty or field missing")
        return 1
    time.sleep(secs)
    t1 = time.time()
    b = newest_as_of(topic)
    if b is None:
        print("EVENT-TIME second sample failed")
        return 1

    wall = (t1 - t0)
    ev = (b - a) / 1000.0
    ratio = ev / wall if wall else 0
    lat0 = t0 - a / 1000.0
    lat1 = t1 - b / 1000.0

    print(f"window {wall:.0f}s wall, topic {topic}")
    print(f"  event time advanced : {ev:>8.1f}s")
    print(f"  progress ratio      : {ratio:>8.2f}   (1.0 = keeping up)")
    print(f"  end-to-end latency  : {lat0:>8.1f}s -> {lat1:.1f}s")
    print("")
    if ratio >= 0.98:
        print("  KEEPING UP: source time advances as fast as real time")
    elif ratio > 0:
        print(f"  FALLING BEHIND: processing {ratio:.0%} of real time. "
              f"Latency grows {(1-ratio)*60:.0f}s per minute.")
    else:
        print("  STALLED: event time is not advancing")
    return 0


if __name__ == "__main__":
    sys.exit(main())
