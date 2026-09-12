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

# --- display-only lookups (new: used for badges/icons, no effect on data/logic) ---
LEVEL_ORDER = ["CRITICAL", "ERROR", "WARNING", "INFO"]
LEVEL_BADGE_COLOR = {
    "CRITICAL": "red",
    "ERROR": "orange",
    "WARNING": "yellow",
    "INFO": "blue",
}
LEVEL_ICON = {
    "CRITICAL": ":material/error:",
    "ERROR": ":material/warning:",
    "WARNING": ":material/info:",
    "INFO": ":material/check_circle:",
}


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


st.set_page_config(page_title="LogStream AI", page_icon=":material/monitoring:", layout="wide")

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


# ============================== Sidebar: controls ==============================
with st.sidebar:
    st.markdown("## LogStream AI")
    st.caption("AI-powered anomaly diagnosis on streaming logs")
    st.caption("Confluent Cloud + Claude")

    with st.container(horizontal=True):
        do_refresh = st.button("Refresh", icon=":material/refresh:", width="stretch")
        do_clear = st.button("Clear", icon=":material/delete_sweep:", width="stretch")

    auto = st.toggle("Auto-refresh every 5s", value=False)

    st.caption("Filters apply to the live feed below")
    service_filter_slot = st.container()
    level_filter_slot = st.container()
    search = st.text_input(
        "Search",
        placeholder="Search message or diagnosis",
        icon=":material/search:",
        label_visibility="collapsed",
    )

if do_clear:
    save_cached_alerts([])

new_count = 0
if do_refresh or auto:
    new_count = poll_and_persist()

alerts = load_cached_alerts()

# --- header ---
st.title("Real-time log diagnosis", icon=":material/monitoring:")
st.caption("Last checked at " + time.strftime("%H:%M:%S"))

# --- errors ---
if st.session_state.get("errors"):
    st.error("Deserialization error: " + st.session_state["errors"][0], icon=":material/error:")

# --- sidebar filter widgets (populated now that alerts are loaded) ---
services = sorted({a.get("service_name", "unknown") for a in alerts})
levels_present = [lvl for lvl in LEVEL_ORDER if any(a.get("log_level") == lvl for a in alerts)]

with service_filter_slot:
    selected_services = st.multiselect("Service", services, placeholder="All services")
with level_filter_slot:
    selected_levels = st.pills("Level", levels_present, selection_mode="multi")


def matches_filters(a):
    if selected_services and a.get("service_name", "unknown") not in selected_services:
        return False
    if selected_levels and a.get("log_level") not in selected_levels:
        return False
    if search:
        needle = search.lower()
        haystack = str(a.get("message", "")) + " " + str(a.get("diagnosis", ""))
        if needle not in haystack.lower():
            return False
    return True


filtered_alerts = [a for a in alerts if matches_filters(a)]

# --- metrics ---
svc_counts = Counter(a.get("service_name", "unknown") for a in alerts)
response_times = [
    a.get("response_time_ms") for a in alerts if isinstance(a.get("response_time_ms"), (int, float))
]
avg_response = sum(response_times) / len(response_times) if response_times else None
# oldest -> newest for the last 20 samples (alerts are newest-first)
response_sparkline = list(reversed(response_times[:20]))

metric_cols = st.columns(5)
with metric_cols[0]:
    st.metric("Total alerts", len(alerts), border=True, icon=":material/notifications:", height="stretch")
with metric_cols[1]:
    st.metric(
        "Top affected service",
        svc_counts.most_common(1)[0][0] if svc_counts else "-",
        border=True,
        icon=":material/dns:",
        height="stretch",
    )
with metric_cols[2]:
    st.metric(
        "Critical alerts",
        sum(1 for a in alerts if a.get("log_level") == "CRITICAL"),
        border=True,
        icon=":material/error:",
        height="stretch",
    )
with metric_cols[3]:
    st.metric("New this refresh", new_count, border=True, icon=":material/bolt:", height="stretch")
with metric_cols[4]:
    st.metric(
        "Avg response time",
        f"{avg_response:.0f} ms" if avg_response is not None else "-",
        border=True,
        icon=":material/speed:",
        chart_data=response_sparkline if response_sparkline else None,
        chart_type="line",
        height="stretch",
    )

# ============================== Tabs: feed / analytics ==============================
tab_feed, tab_analytics = st.tabs([":material/rss_feed: Live feed", ":material/query_stats: Analytics"])

with tab_analytics:
    if svc_counts:
        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.subheader("Alerts by service", icon=":material/bar_chart:")
                try:
                    import pandas as pd
                    df = pd.DataFrame(list(svc_counts.items()), columns=["service", "alerts"])
                    st.bar_chart(df, x="service", y="alerts")
                except Exception:
                    for svc, ct in svc_counts.most_common():
                        st.write("- {}: {}".format(svc, ct))
        with col2:
            with st.container(border=True):
                st.subheader("Alerts by level", icon=":material/warning:")
                level_counts = Counter(a.get("log_level", "UNKNOWN") for a in alerts)
                try:
                    import pandas as pd
                    df2 = pd.DataFrame(
                        [(lvl, level_counts[lvl]) for lvl in LEVEL_ORDER if lvl in level_counts]
                        + [(lvl, ct) for lvl, ct in level_counts.items() if lvl not in LEVEL_ORDER],
                        columns=["level", "alerts"],
                    )
                    st.bar_chart(df2, x="level", y="alerts")
                except Exception:
                    for lvl, ct in level_counts.most_common():
                        st.write("- {}: {}".format(lvl, ct))
    else:
        st.caption("No data yet for analytics.")

    with st.container(border=True):
        st.subheader("Raw cached alerts", icon=":material/table_chart:")
        if alerts:
            st.dataframe(alerts, width="stretch", hide_index=True)
            st.download_button(
                "Download as JSON",
                data=json.dumps(alerts, indent=2, default=str),
                file_name="logstream_alerts.json",
                mime="application/json",
                icon=":material/download:",
            )
        else:
            st.caption("No alerts cached yet.")

with tab_feed:
    st.caption("Showing {} of {} cached alerts".format(len(filtered_alerts), len(alerts)))

    if not alerts:
        st.info(
            "No alerts yet. Click 'Refresh' in the sidebar. Ensure producer + Flink INSERT job are running.",
            icon=":material/info:",
        )
    elif not filtered_alerts:
        st.info("No alerts match the current filters.", icon=":material/filter_alt_off:")
    else:
        for a in filtered_alerts[:25]:
            service = a.get("service_name", "unknown")
            level = a.get("log_level", "?")
            message = a.get("message", "")
            rt = a.get("response_time_ms", "")
            diagnosis = a.get("diagnosis", "No diagnosis available")
            header = "{} - {} ({}ms)".format(service, message, rt)
            try:
                with st.expander(header, icon=LEVEL_ICON.get(level, ":material/notifications:")):
                    with st.container(horizontal=True):
                        st.badge(level, color=LEVEL_BADGE_COLOR.get(level, "gray"))
                        st.badge(service, icon=":material/dns:", color="gray")
                        st.badge("{}ms".format(rt), icon=":material/speed:", color="gray")
                    st.markdown("**AI diagnosis**")
                    st.markdown(diagnosis)
            except Exception:
                st.markdown("### [{}] ".format(level) + header)
                st.markdown("**AI Diagnosis:** " + str(diagnosis))
                st.write("---")

if auto:
    time.sleep(5)
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()
