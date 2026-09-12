#!/usr/bin/env python3
"""
LogStream AI - Real-Time Dashboard (persistent state version)
Alerts are cached to a temp file so they survive Streamlit reruns.

Run:
  python3 -m streamlit run dashboard.py
"""

import json
import os
import tempfile
import time
from collections import Counter

import streamlit as st
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

ALERTS_TOPIC = "ai_alerts_stream"
CACHE_FILE = os.path.join(tempfile.gettempdir(), "logstream_alerts.json")
MAX_ALERTS = 100

CONFIG_PATH = os.environ.get(
    "CONFLUENT_ENV",
    os.path.join(os.path.dirname(__file__), "..", "config", "confluent.env"),
)


def load_env(path):
    cfg = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    cfg[k.strip()] = v.strip()
    return cfg


def load_cached_alerts():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            return []
    return []


def save_cached_alerts(alerts_list):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(alerts_list[:MAX_ALERTS], f, default=str)
    except Exception:
        pass


st.set_page_config(page_title="LogStream AI", page_icon="!", layout="wide")
st.title("LogStream AI - Real-Time Log Diagnosis")
st.caption("AI-powered anomaly diagnosis on streaming logs - Confluent Cloud + Claude")

cfg = load_env(CONFIG_PATH)

# --- init (once per process) ---
if "consumer" not in st.session_state:
    st.session_state.consumer = Consumer({
        "bootstrap.servers": cfg["BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": cfg["KAFKA_API_KEY"],
        "sasl.password": cfg["KAFKA_API_SECRET"],
        "group.id": "logstream-dashboard-v4",
        "auto.offset.reset": "earliest",
    })
    st.session_state.consumer.subscribe([ALERTS_TOPIC])

if "deserializer" not in st.session_state:
    sr_client = SchemaRegistryClient({
        "url": cfg["SR_URL"],
        "basic.auth.user.info": "{}:{}".format(cfg["SR_API_KEY"], cfg["SR_API_SECRET"]),
    })
    st.session_state.deserializer = AvroDeserializer(sr_client)


def poll_and_persist(max_msgs=200):
    """Poll new messages, prepend them to the cached list on disk."""
    consumer = st.session_state.consumer
    deserializer = st.session_state.deserializer

    new_ones = []
    empty_polls = 0
    for _ in range(max_msgs):
        msg = consumer.poll(0.5)
        if msg is None:
            empty_polls += 1
            if empty_polls >= 3:
                break
            continue
        empty_polls = 0
        if msg.error():
            continue
        try:
            rec = deserializer(
                msg.value(),
                SerializationContext(ALERTS_TOPIC, MessageField.VALUE),
            )
            if rec:
                new_ones.append(rec)
        except Exception as e:
            st.session_state.setdefault("errors", [])
            if len(st.session_state["errors"]) < 3:
                st.session_state["errors"].append(str(e))

    # merge: newest first, capped
    cached = load_cached_alerts()
    merged = list(reversed(new_ones)) + cached
    save_cached_alerts(merged)
    return len(new_ones)


# --- controls ---
col_a, col_b, col_c = st.columns([1, 1, 3])
with col_a:
    do_refresh = st.button("Refresh now")
with col_b:
    do_clear = st.button("Clear feed")
with col_c:
    auto = st.checkbox("Auto-refresh every 5s", value=False)

if do_clear:
    save_cached_alerts([])

new_count = 0
if do_refresh or auto:
    new_count = poll_and_persist()

alerts = load_cached_alerts()

# --- errors ---
if st.session_state.get("errors"):
    st.error("Deserialization error: " + st.session_state["errors"][0])

# --- metrics ---
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Alerts", len(alerts))
svc_counts = Counter(a.get("service_name", "unknown") for a in alerts)
c2.metric("Top Affected Service", svc_counts.most_common(1)[0][0] if svc_counts else "-")
c3.metric("Critical Alerts", sum(1 for a in alerts if a.get("log_level") == "CRITICAL"))
c4.metric("New This Refresh", new_count)

st.write("---")

# --- chart (pandas-compatible) ---
if svc_counts:
    st.subheader("Alerts by Service")
    try:
        import pandas as pd
        df = pd.DataFrame(list(svc_counts.items()), columns=["service", "alerts"]).set_index("service")
        st.bar_chart(df)
    except Exception:
        for svc, ct in svc_counts.most_common():
            st.write("- {}: {}".format(svc, ct))

st.write("---")
st.subheader("Live Alert Feed")

if not alerts:
    st.info("No alerts yet. Click 'Refresh now'. Ensure producer + Flink INSERT job are running.")
else:
    for a in alerts[:25]:
        service = a.get("service_name", "unknown")
        level = a.get("log_level", "?")
        message = a.get("message", "")
        rt = a.get("response_time_ms", "")
        diagnosis = a.get("diagnosis", "No diagnosis available")
        header = "[{}] {} - {} ({}ms)".format(level, service, message, rt)
        try:
            with st.expander(header):
                st.markdown("**Service:** `{}` | **Level:** `{}` | **Response:** `{}ms`".format(service, level, rt))
                st.markdown("**AI Diagnosis:**")
                st.markdown(diagnosis)
        except Exception:
            st.markdown("### " + header)
            st.markdown("**AI Diagnosis:** " + str(diagnosis))
            st.write("---")

if auto:
    time.sleep(5)
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()
