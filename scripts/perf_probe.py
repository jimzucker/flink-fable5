#!/usr/bin/env python3
"""
Performance probe: samples Prometheus for N seconds, then measures end-to-end
latency from fresh output records (consume time - trade event time).

Usage: python3 scripts/perf_probe.py <label> [seconds]
"""
import json
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request

PROM = "http://localhost:9090/api/v1/query"

QUERIES = {
    "trades_parsed_per_sec": 'sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond{operator_name="parse_trade"})',
    "prices_parsed_per_sec": 'sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond{operator_name="parse_price"})',
    "dedup_out_per_sec": 'sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond{operator_name="dedup_by_trade_id"})',
    "mv_account_out_per_sec": 'sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond{operator_name="mv_by_account_ticker"})',
    "sink_bytes_per_sec": "sum(flink_taskmanager_job_task_operator_user_demoBytesOutPerSecond)",
    "busy_ms_per_sec_max": "max(flink_taskmanager_job_task_busyTimeMsPerSecond)",
    "backpressure_ms_per_sec_max": "max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)",
    "kafka_pending_records": "sum(flink_taskmanager_job_task_operator_pendingRecords)",
}


def prom(query):
    url = PROM + "?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=10) as resp:
        data = json.load(resp)
    results = data["data"]["result"]
    return float(results[0]["value"][1]) if results else 0.0


def measure_latency(sample_seconds=8):
    """
    True write latency per record: Kafka record CreateTime (stamped when the
    sink produced it) minus as_of (trade event time at generation). No
    consume-window skew.
    """
    # --max-messages exits on a busy stream; --timeout-ms covers a quiet one
    # (it only fires when NO message arrives); subprocess timeout is the belt.
    try:
        proc = subprocess.run(
            ["docker", "compose", "exec", "-T", "kafka",
             "/opt/kafka/bin/kafka-console-consumer.sh",
             "--bootstrap-server", "localhost:9092",
             "--topic", "position-by-account-ticker",
             "--property", "print.timestamp=true",
             "--max-messages", "200",
             "--timeout-ms", str(sample_seconds * 1000)],
            capture_output=True, text=True, timeout=sample_seconds + 25)
        stdout = proc.stdout
    except subprocess.TimeoutExpired as e:
        stdout = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
    latencies = []
    for line in stdout.splitlines():
        # format: CreateTime:1785593564103\t{json}
        if line.startswith("CreateTime:") and "{" in line:
            try:
                ts_part, json_part = line.split("\t", 1)
                write_ts = int(ts_part.split(":")[1])
                rec = json.loads(json_part)
                latencies.append(max(0.0, write_ts - rec["as_of"]))
            except (ValueError, json.JSONDecodeError, KeyError):
                pass
    return latencies


def main():
    label = sys.argv[1] if len(sys.argv) > 1 else "probe"
    seconds = int(sys.argv[2]) if len(sys.argv) > 2 else 30

    samples = {k: [] for k in QUERIES}
    t_end = time.time() + seconds
    while time.time() < t_end:
        for key, query in QUERIES.items():
            try:
                samples[key].append(prom(query))
            except Exception:
                pass
        time.sleep(5)

    print(f"\n=== PERF PROBE: {label} ({seconds}s window) ===")
    for key, values in samples.items():
        if values:
            print(f"  {key:32s} avg={statistics.mean(values):12.1f}  max={max(values):12.1f}")

    latencies = measure_latency()
    if latencies:
        latencies.sort()
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[int(len(latencies) * 0.95) - 1] if len(latencies) > 1 else latencies[0]
        print(f"  {'e2e_latency_ms (trade->output)':32s} p50={p50:9.0f}     p95={p95:9.0f}     n={len(latencies)}")
    else:
        print("  e2e_latency: no fresh records in window")


if __name__ == "__main__":
    main()
