#!/usr/bin/env python3
"""
CR-1 acceptance test: outputs must not update faster than a person can read.

  positions     <= 1 update per key per 500 ms   (<= 2.0/sec)
  market values <= 1 update per key per 1000 ms  (<= 1.0/sec)

Consumes each output topic live for a sampling window and measures the actual
per-key update rate, reporting the worst offender rather than an average — a
mean can look fine while one hot key floods the screen.

Usage:
  Confluent: python3 scripts/cadence_check.py --seconds 60
             (reads config/confluent.properties)
  AWS/local: python3 scripts/cadence_check.py --bootstrap <brokers> [--iam]

Requires: confluent-kafka
"""
import argparse
import json
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

try:
    from confluent_kafka import Consumer
except ImportError:
    sys.exit("confluent-kafka not installed — pip install confluent-kafka")

REPO = Path(__file__).resolve().parent.parent
PROPS_FILE = REPO / "config" / "confluent.properties"

# topic -> (max updates per key per second, human label)
LIMITS = {
    "position-by-account-ticker": (2.0, "positions by account+ticker"),
    "position-by-ticker":         (2.0, "positions by ticker"),
    "mv-by-account-ticker":       (1.0, "market values by account+ticker"),
    "mv-by-ticker":               (1.0, "market values by ticker"),
}
# a little headroom: timers fire on a best-effort schedule and a sampling
# window can clip a boundary, so allow 15% over before calling it a failure
TOLERANCE = 1.15


def confluent_conf():
    if not PROPS_FILE.exists():
        sys.exit(f"missing {PROPS_FILE} — deploy infra-confluent first")
    props = dict(l.split("=", 1) for l in PROPS_FILE.read_text().splitlines()
                 if "=" in l and not l.startswith("#"))
    jaas = props.get("sasl.jaas.config", "")
    user = re.search(r"username='([^']*)'", jaas)
    pw = re.search(r"password='([^']*)'", jaas)
    if not (user and pw):
        sys.exit("could not parse credentials from config/confluent.properties")
    return {"bootstrap.servers": props["bootstrap.servers"],
            "security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN",
            "sasl.username": user.group(1), "sasl.password": pw.group(1)}


def measure(conf, topic, seconds):
    """Return (updates_per_key_per_sec worst, mean, keys, total) sampled live."""
    consumer = Consumer({**conf, "group.id": f"cadence-{topic}-{int(time.time())}",
                         "auto.offset.reset": "latest", "enable.auto.commit": False})
    consumer.subscribe([topic])
    counts = defaultdict(int)
    total = 0
    # let the assignment settle so we do not clip the start of the window
    deadline = time.time() + 5
    while time.time() < deadline:
        consumer.poll(timeout=0.5)
    start = time.time()
    end = start + seconds
    while time.time() < end:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error() or msg.key() is None:
            continue
        counts[msg.key().decode("utf-8", "replace")] += 1
        total += 1
    elapsed = time.time() - start
    consumer.close()
    if not counts:
        return 0.0, 0.0, 0, 0
    rates = [c / elapsed for c in counts.values()]
    return max(rates), sum(rates) / len(rates), len(counts), total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--bootstrap", default=None, help="use AWS/local brokers instead of Confluent")
    ap.add_argument("--iam", action="store_true", help="MSK IAM auth")
    args = ap.parse_args()

    if args.bootstrap:
        conf = {"bootstrap.servers": args.bootstrap}
        if args.iam:
            conf.update({"security.protocol": "SASL_SSL", "sasl.mechanisms": "OAUTHBEARER"})
    else:
        conf = confluent_conf()

    print(f"CR-1 cadence check — sampling {args.seconds}s per topic "
          f"(worst key, not average)\n")
    print(f"{'topic':32s} {'limit/s':>8s} {'worst/s':>9s} {'mean/s':>8s} {'keys':>6s}  verdict")
    ok = True
    for topic, (limit, _label) in LIMITS.items():
        worst, mean, keys, total = measure(conf, topic, args.seconds)
        if total == 0:
            print(f"{topic:32s} {limit:8.1f} {'—':>9s} {'—':>8s} {0:6d}  NO DATA")
            ok = False
            continue
        passed = worst <= limit * TOLERANCE
        ok &= passed
        print(f"{topic:32s} {limit:8.1f} {worst:9.2f} {mean:8.2f} {keys:6d}  "
              f"{'PASS' if passed else 'FAIL'}")
    print("\nRESULT:", "CR-1 SATISFIED" if ok else "CR-1 VIOLATED — see FAIL rows")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
