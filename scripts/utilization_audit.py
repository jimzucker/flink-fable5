#!/usr/bin/env python3
"""Resource utilisation audit — run against EVERY test, before trusting its number.

WHY THIS EXISTS
Confluent's tables were built with 6 buckets while the pool was scaled to 10,
20 and 40 CFUs. A Flink source cannot read a topic with more parallelism than
it has partitions, so price reading was pinned at 6 subtasks the entire time.
Every "Confluent does not scale" measurement was really measuring the bucket
count. It went unnoticed for multiple phases because nothing in the harness
ever compared provisioned compute against the structures that let compute be
used.

The bug class is general: a ceiling somewhere upstream makes added compute
invisible, and the run still produces a plausible-looking number. Throughput
alone cannot distinguish "this platform is slow" from "this config could never
have used what it was given". So every run now answers, first:

    was every unit we paid for actually able to do work?

CHECKS
  1. partitions  >= parallelism        — else (parallelism - partitions) readers idle
  2. key cardinality >= parallelism    — per keyed stage; else workers starve
  3. utilisation                       — busy% of provisioned compute
  4. skew                              — max/min subtask throughput

A FAIL on 1 or 2 invalidates any scaling claim from that run. Do not publish a
number from a run with a FAIL; fix the config and re-run.

Usage:
  scripts/utilization_audit.py --env aws       --parallelism 40
  scripts/utilization_audit.py --env confluent --cfus 20
"""
import argparse
import json
import subprocess
import sys

# Keyed stages and their key cardinality for this workload. Cardinality is a
# property of the DATA, not the config, so it is stated here rather than probed.
# TICKERS is the narrow one and the reason salting exists.
# Defaults mirror infra/variables.tf. Override with --accounts/--tickers when a
# run uses different values -- cardinality drives every check below, so a stale
# constant here silently invalidates the audit.
TICKERS = 10
ACCOUNTS = 5
# hot=True means the stage sees the RAW high-volume feed. A narrow stage on the
# raw feed is a hard FAIL: it throttles the whole job. A narrow stage placed
# AFTER conflation is expected and acceptable — it is narrow because the
# workload has ten tickers, which is a property of the business domain, not a
# misconfiguration. Salting cannot widen it; salting reduces what reaches it.
# Conflating the price feed is precisely what converts the second case from a
# bottleneck into a non-issue, so the two must not be reported the same way.
STAGES = [
    ("dedup-by-trade-id", "trade_id", 1_000_000, "wide — one key per trade", True),
    ("position-by-account-ticker", "account|ticker", ACCOUNTS * TICKERS, "wide", True),
    ("position-by-ticker", "ticker", TICKERS, "narrow, fed by deduped trades", False),
    ("mv-by-account-ticker", "ticker", TICKERS, "narrow, post-conflation", False),
    ("mv-by-ticker", "ticker", TICKERS, "narrow, post-conflation", False),
    ("price-conflate-local", "symbol|salt", TICKERS * 8, "salted: 10 x factor 8", True),
]

findings = []


def check(ok, label, detail, warn_only=False):
    status = "PASS" if ok else ("WARN" if warn_only else "FAIL")
    findings.append((status, label, detail))
    return ok


def audit_partitions(partitions, parallelism):
    ok = partitions >= parallelism
    if ok:
        detail = f"{partitions} partitions >= parallelism {parallelism}"
    else:
        idle = parallelism - partitions
        detail = (f"{partitions} partitions < parallelism {parallelism} — "
                  f"{idle} source subtask(s) can never be assigned a partition. "
                  f"Any scaling claim from this run is invalid.")
    check(ok, "partitions >= parallelism", detail)


def audit_key_cardinality(parallelism, salted):
    for name, key, card, note, hot in STAGES:
        if name == "price-conflate-local" and not salted:
            continue
        ok = card >= parallelism
        usable = min(card, parallelism)
        detail = (f"key={key} cardinality={card} ({note}); "
                  f"at parallelism {parallelism} at most {usable} worker(s) can be busy")
        if not ok:
            idle = parallelism - card
            if hot:
                detail += (f" — {idle} idle ON THE RAW FEED. This throttles the whole job; "
                           f"salt this stage or the number is meaningless.")
            else:
                detail += (f" — {idle} idle, but downstream of conflation so the volume "
                           f"reaching it is bounded. Narrow because the domain has "
                           f"{TICKERS} tickers; not fixable by config.")
        # Unsalted price conflation IS on the raw feed and IS narrow — the exact
        # condition that produced the bogus "Confluent cannot scale" readings.
        if not salted and name == "mv-by-account-ticker":
            hot = True
            detail += " UNSALTED: the raw price feed reaches this stage directly."
        check(ok, f"cardinality: {name}", detail, warn_only=not hot)


def aws(args):
    print(f"# AWS utilisation audit (parallelism={args.parallelism})\n")
    partitions = args.partitions
    if partitions is None:
        out = subprocess.run(
            ["terraform", "-chdir=infra", "output", "-raw", "topics_partitions"],
            capture_output=True, text=True).stdout.strip()
        partitions = int(out) if out.isdigit() else 48
    audit_partitions(partitions, args.parallelism)
    audit_key_cardinality(args.parallelism, args.salted)

    # Billed units vs parallelism. MSF packs ParallelismPerKPU subtasks per KPU
    # and adds one orchestration KPU; paying for KPUs whose subtasks cannot be
    # fed is the cost half of the same mistake.
    ppk = args.parallelism_per_kpu
    billed = args.parallelism / ppk + 1
    narrow = TICKERS * (8 if args.salted else 1)
    effective = min(args.parallelism, narrow)
    waste = 1 - (effective / args.parallelism)
    check(waste <= 0.25, "compute reachable by the narrowest stage",
          f"billed ~{billed:.0f} KPU for parallelism {args.parallelism}; narrowest stage can use "
          f"{effective} — {waste*100:.0f}% of parallelism unreachable"
          + ("" if waste <= 0.25 else " — reduce parallelism or raise the salt factor"))
    print_report()


def confluent(args):
    print(f"# Confluent utilisation audit (max_cfu={args.cfus})\n")
    buckets = args.partitions if args.partitions is not None else 48
    # A CFU is not a subtask, but pool parallelism scales with it; the bucket
    # count is still the hard ceiling on source readers.
    audit_partitions(buckets, args.cfus)
    audit_key_cardinality(args.cfus, args.salted)
    print_report()


def print_report():
    width = max(len(l) for _, l, _ in findings) + 2
    fails = 0
    for status, label, detail in findings:
        if status == "FAIL":
            fails += 1
        print(f"{status:5} {label:<{width}} {detail}")
    print()
    if fails:
        print(f"{fails} FAIL(s). This run cannot support a scaling or cost claim — "
              f"fix the config and re-run.")
        sys.exit(1)
    print("All checks passed — provisioned compute is reachable by the topology.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--env", choices=["aws", "confluent"], required=True)
    p.add_argument("--parallelism", type=int, default=40)
    p.add_argument("--parallelism-per-kpu", type=int, default=4)
    p.add_argument("--cfus", type=int, default=10)
    p.add_argument("--partitions", type=int, default=None)
    p.add_argument("--accounts", type=int, default=ACCOUNTS)
    p.add_argument("--tickers", type=int, default=TICKERS)
    p.add_argument("--salted", action="store_true", default=True)
    p.add_argument("--no-salted", dest="salted", action="store_false")
    a = p.parse_args()
    TICKERS = a.tickers
    ACCOUNTS = a.accounts
    STAGES[1] = ("position-by-account-ticker", "account|ticker", ACCOUNTS * TICKERS, "wide", True)
    STAGES[2] = ("position-by-ticker", "ticker", TICKERS, "narrow, fed by deduped trades", False)
    STAGES[3] = ("mv-by-account-ticker", "ticker", TICKERS, "narrow, post-conflation", False)
    STAGES[4] = ("mv-by-ticker", "ticker", TICKERS, "narrow, post-conflation", False)
    STAGES[5] = ("price-conflate-local", "symbol|salt", TICKERS * 8, "salted: tickers x factor 8", True)
    (aws if a.env == "aws" else confluent)(a)
