#!/usr/bin/env python3
"""
LogStream AI - Log Producer for Confluent Cloud
Runs on your Rocky Linux VM. Streams realistic application logs to the `raw-logs` topic.

Credentials are read from config/confluent.env (kept out of the code).

Usage:
  Normal traffic:   python3 log_producer.py
  Trigger a spike:  python3 log_producer.py --spike        (30s error burst, then back to normal)
  Adjust rate:      python3 log_producer.py --rate 20      (20 logs/sec, default is 5)
  Custom config:    python3 log_producer.py --env ../config/confluent.env

Press Ctrl+C to stop.
"""

import argparse
import json
import os
import random
import signal
import sys
import time

from confluent_kafka import Producer


# ============================================================
# Load config from an env file (KEY=VALUE lines)
# ============================================================
def load_env(path):
    if not os.path.exists(path):
        print(f"ERROR: config file not found: {path}")
        print("Create it with your BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET, TOPIC.")
        sys.exit(1)
    cfg = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            cfg[key.strip()] = val.strip()
    required = ["BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET"]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        print(f"ERROR: missing keys in {path}: {', '.join(missing)}")
        sys.exit(1)
    cfg.setdefault("TOPIC", "raw-logs")
    return cfg


# ============================================================
# Log data definitions (matches your Datagen schema)
# ============================================================
SERVICES = ["auth-service", "payment-service", "user-service",
            "order-service", "notification-service", "api-gateway"]
HOSTS = ["host-01", "host-02", "host-03", "host-04"]

NORMAL_LEVELS = ["INFO", "INFO", "INFO", "INFO", "INFO", "WARN", "WARN", "ERROR", "CRITICAL"]
SPIKE_LEVELS  = ["ERROR", "ERROR", "ERROR", "CRITICAL", "CRITICAL", "WARN"]

MESSAGES = {
    "INFO":     ["Request processed successfully", "User authenticated",
                 "Database query completed", "Cache miss, fetching from origin",
                 "Health check passed"],
    "WARN":     ["Rate limit exceeded for client", "Cache miss, fetching from origin",
                 "Slow query detected"],
    "ERROR":    ["Connection timeout to upstream service",
                 "Failed to acquire database connection",
                 "Null pointer exception in handler"],
    "CRITICAL": ["Payment gateway returned error 500", "Service unavailable",
                 "Out of memory error"],
}


def make_log(spike=False):
    level = random.choice(SPIKE_LEVELS if spike else NORMAL_LEVELS)
    return {
        "service_name": random.choice(SERVICES),
        "log_level": level,
        "host": random.choice(HOSTS),
        "message": random.choice(MESSAGES[level]),
        "request_id": random.randint(1, 1_000_000),
        "response_time_ms": random.randint(1500, 5000) if spike else random.randint(5, 3000),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"  ! Delivery failed: {err}")


def build_producer(cfg):
    return Producer({
        "bootstrap.servers": cfg["BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": cfg["KAFKA_API_KEY"],
        "sasl.password": cfg["KAFKA_API_SECRET"],
        "client.id": "logstream-vm-producer",
    })


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=os.path.join(os.path.dirname(__file__), "..", "config", "confluent.env"),
                        help="path to config env file")
    parser.add_argument("--rate", type=float, default=5.0, help="logs per second (default 5)")
    parser.add_argument("--spike", action="store_true", help="produce a spike then return to normal")
    parser.add_argument("--spike-seconds", type=int, default=30, help="duration of spike in seconds")
    args = parser.parse_args()

    cfg = load_env(args.env)
    topic = cfg["TOPIC"]
    producer = build_producer(cfg)

    running = {"on": True}
    def stop(sig, frame):
        print("\nFlushing and stopping...")
        running["on"] = False
    signal.signal(signal.SIGINT, stop)

    interval = 1.0 / args.rate
    sent = 0
    start = time.time()
    spike_until = (start + args.spike_seconds) if args.spike else 0

    mode = "SPIKE" if args.spike else "NORMAL"
    print(f"Producing to '{topic}' at {args.rate} logs/sec  [mode: {mode}]  (Ctrl+C to stop)")

    while running["on"]:
        in_spike = time.time() < spike_until
        record = make_log(spike=in_spike)

        producer.produce(
            topic,
            key=record["service_name"],
            value=json.dumps(record),
            callback=delivery_report,
        )
        producer.poll(0)
        sent += 1

        if sent % 50 == 0:
            tag = " (SPIKE)" if in_spike else ""
            print(f"  sent {sent} logs{tag}")

        if args.spike and spike_until and time.time() >= spike_until:
            print("  >> spike over, back to normal traffic")
            spike_until = 0

        time.sleep(interval)

    producer.flush()
    print(f"Done. Total sent: {sent}")


if __name__ == "__main__":
    main()
