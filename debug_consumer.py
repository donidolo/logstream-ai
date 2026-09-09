#!/usr/bin/env python3
"""Quick diagnostic: read a few messages from ai_alerts_stream and show their raw format."""
import os
from confluent_kafka import Consumer

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "confluent.env")

def load_env(path):
    cfg = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                cfg[k.strip()] = v.strip()
    return cfg

cfg = load_env(CONFIG_PATH)
c = Consumer({
    "bootstrap.servers": cfg["BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": cfg["KAFKA_API_KEY"],
    "sasl.password": cfg["KAFKA_API_SECRET"],
    "group.id": "debug-inspector",
    "auto.offset.reset": "earliest",
})
c.subscribe(["ai_alerts_stream"])

print("Polling for messages (10s)...")
got = 0
import time
end = time.time() + 10
while time.time() < end and got < 3:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("ERR:", msg.error())
        continue
    got += 1
    raw = msg.value()
    print("\n--- Message {} ---".format(got))
    print("First 10 bytes (hex):", raw[:10].hex())
    print("Starts with 0x00 (Avro wire format)?", raw[0] == 0)
    print("First 60 bytes (repr):", repr(raw[:60]))

if got == 0:
    print("\nNo messages received. Either topic is empty or consumer group already consumed them.")
c.close()
