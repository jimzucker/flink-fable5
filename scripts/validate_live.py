#!/usr/bin/env python3
"""
Live validation against the running stack (make validate).

Independently recomputes every output from the raw trades/prices topics and
compares with what the pipeline actually published:
  1. dedup        — duplicates in input vs distinct trade_ids
  2. reproducible — recomputed positions == pipeline positions (account+ticker)
  3. reproducible — recomputed positions == pipeline positions (ticker)
  4. completeness — sum(account positions) == ticker position
  5. market value — final MV == final position x final price (exact decimals)

The generator is paused during the check so the pipeline can drain to a
consistent final state, then restarted.
"""
import json
import subprocess
import sys
from decimal import Decimal

COMPOSE = ["docker", "compose"]


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def dump_topic(topic, timeout_ms=8000):
    result = sh(COMPOSE + [
        "exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "localhost:9092", "--topic", topic,
        "--from-beginning", "--timeout-ms", str(timeout_ms)])
    records = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return records


def check(name, ok, detail):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    return ok


def main():
    print("Pausing generator and letting the pipeline drain...")
    sh(COMPOSE + ["stop", "generator"])
    subprocess.run(["sleep", "10"])

    print("Dumping topics...")
    trades = dump_topic("trades")
    pos_acct = dump_topic("position-by-account-ticker")
    pos_tick = dump_topic("position-by-ticker")
    mv_acct = dump_topic("mv-by-account-ticker")
    mv_tick = dump_topic("mv-by-ticker")
    prices = dump_topic("prices")

    print(f"  trades={len(trades)} prices={len(prices)} pos_acct={len(pos_acct)} "
          f"pos_tick={len(pos_tick)} mv_acct={len(mv_acct)} mv_tick={len(mv_tick)}")

    # Independent recompute from raw input
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

    # Final snapshot per key from pipeline outputs (upsert streams: last wins)
    actual_acct = {(p["account"], p["ticker"]): p["net_qty"] for p in pos_acct}
    actual_tick = {p["ticker"]: p["net_qty"] for p in pos_tick}
    final_price = {p["symbol"]: Decimal(p["price"]) for p in prices}
    actual_mv_acct = {(m["account"], m["ticker"]): Decimal(m["mv"]) for m in mv_acct}
    actual_mv_tick = {m["ticker"]: Decimal(m["mv"]) for m in mv_tick}

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
    ok &= check("MV by account == position x final price", not mv_bad,
                f"{len(actual_mv_acct)} keys checked exactly" + (f", BAD: {mv_bad[:3]}" if mv_bad else ""))

    mv_bad = []
    for ticker, qty in actual_tick.items():
        if ticker in final_price and ticker in actual_mv_tick:
            expected_mv = final_price[ticker] * qty
            if actual_mv_tick[ticker] != expected_mv:
                mv_bad.append((ticker, str(expected_mv), str(actual_mv_tick[ticker])))
    ok &= check("MV by ticker == position x final price", not mv_bad,
                f"{len(actual_mv_tick)} tickers checked exactly" + (f", BAD: {mv_bad[:3]}" if mv_bad else ""))

    print("\nRestarting generator...")
    sh(COMPOSE + ["start", "generator"])

    print("RESULT:", "ALL CHECKS PASSED" if ok else "FAILURES — see above")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
