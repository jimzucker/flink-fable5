#!/usr/bin/env python3
"""End-to-end latency against Confluent Cloud: event time -> output published.

Latency = the Kafka record timestamp of an output record minus the `as_of`
carried in its payload (the newest input event_time that produced it).

CRITICAL: the pipeline must be CAUGHT UP first. A statement still draining a
backlog reports BACKLOG AGE, not latency -- an earlier phase of this project
read 170s and nearly published it. Start the statement at latest-offset, or
verify lag ~0, before believing any number here.

usage: confluent_latency.py <topic> <seconds>
"""
import json, re, statistics, sys, time
from pathlib import Path
from confluent_kafka import Consumer

PROPS = Path(__file__).resolve().parent.parent / "config" / "confluent.properties"
topic = sys.argv[1] if len(sys.argv) > 1 else "mv-by-account-ticker"
secs = int(sys.argv[2]) if len(sys.argv) > 2 else 120

t = PROPS.read_text()
boot = re.search(r"^bootstrap\.servers=(.+)$", t, re.M).group(1).strip()
jaas = re.search(r"^sasl\.jaas\.config=(.+)$", t, re.M).group(1)
user = re.search(r"username='([^']+)'", jaas).group(1)
pw = re.search(r"password='([^']+)'", jaas).group(1)

c = Consumer({"bootstrap.servers": boot, "security.protocol": "SASL_SSL",
              "sasl.mechanisms": "PLAIN", "sasl.username": user, "sasl.password": pw,
              "group.id": f"latency-probe-{int(time.time())}",
              "auto.offset.reset": "latest", "enable.auto.commit": False})
c.subscribe([topic])
lat, n, end = [], 0, time.time() + secs
while time.time() < end:
    m = c.poll(timeout=1.0)
    if m is None or m.error() or m.value() is None:
        continue
    try:
        v = json.loads(m.value().decode())
        as_of = int(v.get("as_of", 0))
        if as_of <= 0:
            continue
        _, ts = m.timestamp()
        d = ts - as_of
        if -1000 < d < 600000:          # ignore clock nonsense
            lat.append(d)
        n += 1
    except Exception:
        continue
c.close()
if not lat:
    print(f"NO SAMPLES from {topic} in {secs}s — is the pipeline running and fed?")
    sys.exit(1)
lat.sort()
p = lambda q: lat[min(len(lat) - 1, int(len(lat) * q))]
print(f"LATENCY {topic}: n={len(lat)} p50={p(.50)}ms p90={p(.90)}ms "
      f"p99={p(.99)}ms min={lat[0]}ms max={lat[-1]}ms mean={statistics.mean(lat):.0f}ms")
