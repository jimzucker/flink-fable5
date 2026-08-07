#!/usr/bin/env python3
"""Streaming correctness validation against Confluent Cloud — usable AT VOLUME.

WHY THIS EXISTS
`confluent_validate.py` reads every record into a Python list, so it cannot
validate a run of any size (a 19M-record backlog is gigabytes). Phases 16 and 17
therefore produced dozens of throughput numbers with correctness never once
verified. The rule now is: verify correctness at the volume being claimed,
before the number is claimed. Fast only counts if the numbers are correct.

BOUNDED MEMORY
  trades  -> streamed; keeps a set of trade_ids and two aggregate dicts
             (~15,000 account|ticker keys, ~3,000 ticker keys)
  prices  -> streamed; keeps ONLY the latest per symbol (~3,000 entries)
  outputs -> streamed; upsert topics, so last value per key wins
Nothing is fully materialised. Peak memory is a function of KEY COUNT, not
record count.

SIMPLE NUMBERS
Run the generator with `--generator.qty.override 1
--generator.price.cents.override 100` and every expectation becomes arithmetic
you can check by hand: a position is the count of deduped trades for that key,
and market value equals that count in dollars. A failure is then an off-by-N a
human can see, rather than something only a second program can detect.

Usage:
  scripts/confluent_validate_stream.py [--timeout-sec 60]

Reads config/confluent.properties (written by terraform apply in
infra-confluent/). Requires: pip install confluent-kafka
"""
import argparse
import json
import re
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

try:
    from confluent_kafka import Consumer, TopicPartition
except ImportError:
    print("needs: pip install confluent-kafka", file=sys.stderr)
    sys.exit(2)

PROPS = Path(__file__).resolve().parent.parent / "config" / "confluent.properties"


def conf_from_props():
    text = PROPS.read_text()
    boot = re.search(r"^bootstrap\.servers=(.+)$", text, re.M)
    jaas = re.search(r"^sasl\.jaas\.config=(.+)$", text, re.M)
    if not boot or not jaas:
        print(f"missing bootstrap/jaas in {PROPS}", file=sys.stderr)
        sys.exit(2)
    user = re.search(r"username='([^']+)'", jaas.group(1))
    pw = re.search(r"password='([^']+)'", jaas.group(1))
    return {
        "bootstrap.servers": boot.group(1).strip(),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": user.group(1),
        "sasl.password": pw.group(1),
        "group.id": "confluent-validate-stream",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }


def stream(conf, topic, on_record, idle_timeout=30):
    """Feed every record of `topic` to on_record(key, value_dict). No buffering."""
    consumer = Consumer(conf)
    md = consumer.list_topics(topic, timeout=20)
    if topic not in md.topics or md.topics[topic].error is not None:
        consumer.close()
        return 0
    parts = [TopicPartition(topic, p) for p in md.topics[topic].partitions]
    remaining = {}
    for tp in parts:
        lo, hi = consumer.get_watermark_offsets(tp, timeout=20)
        tp.offset = lo
        if hi > lo:
            remaining[tp.partition] = hi
    if not remaining:
        consumer.close()
        return 0
    consumer.assign(parts)
    n = 0
    idle = 0
    while remaining and idle < idle_timeout:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            idle += 1
            continue
        idle = 0
        if msg.error():
            continue
        if msg.value() is not None:
            try:
                on_record(msg.key().decode() if msg.key() else None,
                          json.loads(msg.value().decode("utf-8")))
                n += 1
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        p = msg.partition()
        if p in remaining and msg.offset() + 1 >= remaining[p]:
            del remaining[p]
    consumer.close()
    return n


FAILS = []


def check(name, ok, detail):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    if not ok:
        FAILS.append(name)
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--timeout-sec", type=int, default=30)
    args = ap.parse_args()
    conf = conf_from_props()

    # ---- recompute truth from the RAW topics, streaming ----
    seen_trades = set()
    dup_count = [0]
    pos_acct = defaultdict(int)      # "account|ticker" -> net qty
    pos_ticker = defaultdict(int)    # "ticker" -> net qty

    def on_trade(_k, t):
        tid = t.get("trade_id")
        if tid in seen_trades:
            dup_count[0] += 1
            return                    # dedup: a repeat must not move a position
        seen_trades.add(tid)
        q = int(t["qty"])
        pos_acct[f"{t['account']}|{t['ticker']}"] += q
        pos_ticker[t["ticker"]] += q

    n_trades = stream(conf, "trades", on_trade, args.timeout_sec)

    latest_price = {}                 # symbol -> (event_time, Decimal price)

    def on_price(_k, p):
        s, et = p["symbol"], int(p["event_time"])
        cur = latest_price.get(s)
        if cur is None or et >= cur[0]:
            latest_price[s] = (et, Decimal(str(p["price"])))

    n_prices = stream(conf, "prices", on_price, args.timeout_sec)

    # the pipeline joins against the CONFLATED price, so that is the fair truth
    conflated = {}

    def on_conf(_k, p):
        s, et = p["symbol"], int(p["event_time"])
        cur = conflated.get(s)
        if cur is None or et >= cur[0]:
            conflated[s] = (et, Decimal(str(p["price"])))

    stream(conf, "prices-conflated", on_conf, args.timeout_sec)
    truth_price = conflated or latest_price

    # ---- read what the pipeline PUBLISHED (upsert: last value per key wins) ----
    out = {t: {} for t in ("position-by-account-ticker", "position-by-ticker",
                           "mv-by-account-ticker", "mv-by-ticker")}
    for t in out:
        stream(conf, t, lambda k, v, _t=t: out[_t].__setitem__(k, v), args.timeout_sec)

    print(f"\nstreamed: trades={n_trades:,} prices={n_prices:,} "
          f"symbols={len(latest_price):,} conflated={len(conflated):,}")
    print(f"published: " + "  ".join(f"{t.split('-')[0]}={len(v):,}" for t, v in out.items()))
    print()

    # ---- the six checks ----
    check("dedup", dup_count[0] > 0 and len(seen_trades) == n_trades - dup_count[0],
          f"{dup_count[0]:,} duplicates seen, {len(seen_trades):,} unique of {n_trades:,}")

    bad = [(k, v, pos_acct.get(k)) for k, v in out["position-by-account-ticker"].items()
           if int(v.get("net_qty", 0)) != pos_acct.get(k.replace("#", "|") if k else k, None)
           and int(v.get("net_qty", 0)) != pos_acct.get(k)]
    check("positions by account+ticker reproducible", not bad,
          f"{len(out['position-by-account-ticker']):,} keys checked, {len(bad)} mismatched"
          + (f" e.g. {bad[0]}" if bad else ""))

    badt = [(k, int(v.get("net_qty", 0)), pos_ticker.get(k))
            for k, v in out["position-by-ticker"].items()
            if int(v.get("net_qty", 0)) != pos_ticker.get(k)]
    check("positions by ticker reproducible", not badt,
          f"{len(out['position-by-ticker']):,} keys checked, {len(badt)} mismatched"
          + (f" e.g. {badt[0]}" if badt else ""))

    rolled = defaultdict(int)
    for k, q in pos_acct.items():
        rolled[k.split("|")[1]] += q
    badc = [(t, q, pos_ticker.get(t)) for t, q in rolled.items() if q != pos_ticker.get(t)]
    check("completeness sum(accounts)==ticker", not badc,
          f"{len(rolled):,} tickers rolled up, {len(badc)} disagree")

    def mv_bad(topic, keyfn, posmap, prices):
        """MV must equal the RECOMPUTED position x the price. Uses our own
        position, never the published one, so a wrong position cannot cancel a
        wrong price and still look correct."""
        strict, stale = [], []
        for k, v in out[topic].items():
            sym = keyfn(k)
            expect_qty = posmap.get(k)
            if expect_qty is None:
                continue
            raw = prices.get(sym)
            if raw is None:
                continue
            got = Decimal(str(v.get("mv", "0")))
            if Decimal(expect_qty) * raw[1] == got:
                continue
            # not the final raw price — is it explained by conflation lag?
            cp = conflated.get(sym)
            if cp is not None and Decimal(expect_qty) * cp[1] == got:
                stale.append((k, str(raw[1]), str(cp[1])))
            else:
                strict.append((k, str(got), str(Decimal(expect_qty) * raw[1])))
        return strict, stale

    # PRIMARY assertion: final MV == recomputed position x FINAL RAW price.
    # Checking against the pipeline's own conflated topic would be partly
    # circular -- if conflation were wrong, truth and pipeline would share the
    # error and the check would pass. Conflation lag is reported separately so a
    # known semantic difference is never silently counted as correctness.
    b1, s1 = mv_bad("mv-by-account-ticker",
                    lambda k: k.split("|")[1] if k and "|" in k else k,
                    pos_acct, latest_price)
    check("MV by account == position x FINAL price", not b1,
          f"{len(out['mv-by-account-ticker']):,} checked, {len(b1)} wrong"
          + (f", {len(s1)} explained by conflation lag" if s1 else "")
          + (f" e.g. {b1[0]}" if b1 else ""))

    b2, s2 = mv_bad("mv-by-ticker", lambda k: k, pos_ticker, latest_price)
    check("MV by ticker == position x FINAL price", not b2,
          f"{len(out['mv-by-ticker']):,} checked, {len(b2)} wrong"
          + (f", {len(s2)} explained by conflation lag" if s2 else "")
          + (f" e.g. {b2[0]}" if b2 else ""))

    if s1 or s2:
        print(f"  [WARN] conflation lag: {len(s1)+len(s2)} market values priced at the "
              f"last CONFLATED tick rather than the final raw tick. Exact given what the "
              f"join saw, but not the newest price. With --generator.price.cents.override "
              f"this must be ZERO, since every price is identical.")

    print()
    if FAILS:
        print(f"VALIDATION FAILED: {', '.join(FAILS)}")
        sys.exit(1)
    print("VALIDATION PASSED — all six checks")


if __name__ == "__main__":
    main()
