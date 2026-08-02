#!/usr/bin/env python3
"""
Independent validation against the Confluent Cloud stack — the same five
checks as validate_live.py (local) — recomputing every output from the raw
trades/prices topics and comparing with what the SQL pipeline published.

Usage:
  1. Stop the local generator (Ctrl-C) and wait ~15 s for the statements to
     drain to a consistent final state.
  2. python3 scripts/confluent_validate.py

Requires: pip install confluent-kafka
Reads connection info from config/confluent.properties (written by
`terraform apply` in infra-confluent/).

One semantic difference vs the DataStream version, checked honestly:
the Java conflation timer always flushes 250 ms after the last tick, but a
SQL tumbling window only closes when the watermark passes it — so after
quiesce the very last price tick may still be parked in an unclosed window.
The MV checks therefore use the final *conflated* price as truth (that is
what the join saw), and a separate freshness check WARNs if it lags the
final raw price.
"""
import re
import sys
from decimal import Decimal
from pathlib import Path

try:
    from confluent_kafka import Consumer, TopicPartition
except ImportError:
    sys.exit("confluent-kafka not installed — run: pip install confluent-kafka")

import json

REPO_ROOT = Path(__file__).resolve().parent.parent
PROPS_FILE = REPO_ROOT / "config" / "confluent.properties"

TOPICS = ["trades", "prices", "prices-conflated", "position-by-account-ticker",
          "position-by-ticker", "mv-by-account-ticker", "mv-by-ticker"]


def client_conf():
    if not PROPS_FILE.exists():
        sys.exit(f"missing {PROPS_FILE} — run terraform apply in infra-confluent/ first")
    props = dict(line.split("=", 1) for line in PROPS_FILE.read_text().splitlines()
                 if "=" in line and not line.startswith("#"))
    jaas = props.get("sasl.jaas.config", "")
    user = re.search(r"username='([^']*)'", jaas)
    pw = re.search(r"password='([^']*)'", jaas)
    if not (user and pw):
        sys.exit("could not parse username/password from sasl.jaas.config")
    return {
        "bootstrap.servers": props["bootstrap.servers"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": user.group(1),
        "sasl.password": pw.group(1),
        "group.id": "confluent-validate",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }


def dump_topic(conf, topic):
    """Read a topic from beginning to its current end offsets."""
    consumer = Consumer(conf)
    md = consumer.list_topics(topic, timeout=15)
    if topic not in md.topics or md.topics[topic].error is not None:
        consumer.close()
        return []
    partitions = [TopicPartition(topic, p) for p in md.topics[topic].partitions]
    ends = {}
    for tp in partitions:
        lo, hi = consumer.get_watermark_offsets(tp, timeout=15)
        tp.offset = lo
        ends[tp.partition] = hi
    consumer.assign(partitions)
    remaining = {p: hi for p, hi in ends.items() if hi > 0}
    records = []
    while remaining:
        msg = consumer.poll(timeout=10)
        if msg is None:
            break  # drained (or idle timeout — good enough at demo volumes)
        if msg.error():
            continue
        if msg.value() is not None:
            try:
                records.append(json.loads(msg.value().decode("utf-8")))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        p = msg.partition()
        if p in remaining and msg.offset() + 1 >= remaining[p]:
            del remaining[p]
    consumer.close()
    return records


def check(name, ok, detail):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    return ok


def main():
    conf = client_conf()
    print("Dumping topics from Confluent Cloud...")
    dumps = {t: dump_topic(conf, t) for t in TOPICS}
    trades = dumps["trades"]
    prices = dumps["prices"]
    conflated = dumps["prices-conflated"]
    pos_acct = dumps["position-by-account-ticker"]
    pos_tick = dumps["position-by-ticker"]
    mv_acct = dumps["mv-by-account-ticker"]
    mv_tick = dumps["mv-by-ticker"]

    print(f"  trades={len(trades)} prices={len(prices)} conflated={len(conflated)} "
          f"pos_acct={len(pos_acct)} pos_tick={len(pos_tick)} "
          f"mv_acct={len(mv_acct)} mv_tick={len(mv_tick)}")

    # Independent recompute from raw input (identical to validate_live.py)
    seen, expected_acct, expected_tick = set(), {}, {}
    duplicates = 0
    for t in trades:
        tid = t["trade_id"]
        if tid in seen:
            duplicates += 1
            continue
        seen.add(tid)
        expected_acct[(t["account"], t["ticker"])] = \
            expected_acct.get((t["account"], t["ticker"]), 0) + t["qty"]
        expected_tick[t["ticker"]] = expected_tick.get(t["ticker"], 0) + t["qty"]

    actual_acct = {(p["account"], p["ticker"]): p["net_qty"] for p in pos_acct}
    actual_tick = {p["ticker"]: p["net_qty"] for p in pos_tick}
    final_raw_price = {p["symbol"]: Decimal(str(p["price"])) for p in prices}
    final_price = {p["symbol"]: Decimal(str(p["price"])) for p in conflated}
    actual_mv_acct = {(m["account"], m["ticker"]): Decimal(str(m["mv"])) for m in mv_acct}
    actual_mv_tick = {m["ticker"]: Decimal(str(m["mv"])) for m in mv_tick}

    print("\nValidation:")
    ok = True

    ok &= check("dedup", duplicates > 0 and len(seen) == len(trades) - duplicates,
                f"{len(trades)} trade records, {duplicates} duplicates, {len(seen)} distinct")

    mismatches = {k: (v, actual_acct.get(k)) for k, v in expected_acct.items()
                  if actual_acct.get(k) != v}
    ok &= check("positions by account+ticker reproducible", not mismatches,
                f"{len(expected_acct)} keys recomputed from raw trades"
                + (f", MISMATCHES: {list(mismatches.items())[:3]}" if mismatches else ", all match pipeline output"))

    mismatches = {k: (v, actual_tick.get(k)) for k, v in expected_tick.items()
                  if actual_tick.get(k) != v}
    ok &= check("positions by ticker reproducible", not mismatches,
                f"{len(expected_tick)} tickers recomputed"
                + (f", MISMATCHES: {list(mismatches.items())[:3]}" if mismatches else ", all match pipeline output"))

    summed = {}
    for (_, ticker), qty in actual_acct.items():
        summed[ticker] = summed.get(ticker, 0) + qty
    bad = {t: (summed.get(t), actual_tick.get(t)) for t in actual_tick
           if summed.get(t) != actual_tick.get(t)}
    ok &= check("completeness sum(accounts)==ticker", not bad,
                f"{len(actual_tick)} tickers" + (f", BAD: {bad}" if bad else ", invariant holds"))

    mv_bad = []
    for (account, ticker), qty in actual_acct.items():
        if ticker in final_price and (account, ticker) in actual_mv_acct:
            expected_mv = final_price[ticker] * qty
            if actual_mv_acct[(account, ticker)] != expected_mv:
                mv_bad.append((account, ticker, str(expected_mv), str(actual_mv_acct[(account, ticker)])))
    ok &= check("MV by account == position x final conflated price", not mv_bad,
                f"{len(actual_mv_acct)} keys checked exactly" + (f", BAD: {mv_bad[:3]}" if mv_bad else ""))

    mv_bad = []
    for ticker, qty in actual_tick.items():
        if ticker in final_price and ticker in actual_mv_tick:
            expected_mv = final_price[ticker] * qty
            if actual_mv_tick[ticker] != expected_mv:
                mv_bad.append((ticker, str(expected_mv), str(actual_mv_tick[ticker])))
    ok &= check("MV by ticker == position x final conflated price", not mv_bad,
                f"{len(actual_mv_tick)} tickers checked exactly" + (f", BAD: {mv_bad[:3]}" if mv_bad else ""))

    # Freshness: conflated price vs final raw price (WARN, not FAIL — the
    # last window may lawfully still be open after quiesce; see header).
    stale = {s: (str(final_price.get(s)), str(final_raw_price[s]))
             for s in final_raw_price if final_price.get(s) != final_raw_price[s]}
    if stale:
        print(f"  [WARN] conflated price lags final raw tick for {len(stale)} symbols "
              f"(unclosed last window — expected on quiesce): {list(stale.items())[:3]}")
    else:
        print("  [INFO] conflated prices identical to final raw prices for all symbols")

    print("\nRESULT:", "ALL CHECKS PASSED" if ok else "FAILURES — see above")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
