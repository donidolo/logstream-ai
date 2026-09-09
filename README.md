# LogStream AI

**Real-Time AI-Powered Log & Transaction Diagnosis Platform**

Built for [Confluent AI Day Indonesia 2026](https://events.confluent.io/confluentaiday2026indonesia) Hackathon by Team Espada.

> From raw log to actionable diagnosis in seconds — powered by Confluent Cloud + Claude

## What It Does

LogStream AI continuously ingests application logs and database change events, processes them through Apache Flink SQL with ML-based anomaly detection, then invokes Claude AI to generate actionable root-cause diagnoses for every critical event — all in real-time, with no batch delay.

## Architecture

```
┌─────────────────┐     ┌──────────────────────┐
│ VM Log Producer  │     │ PostgreSQL CDC        │
│ (Python)         │     │ (Debezium Connector)  │
└────────┬────────┘     └──────────┬───────────┘
         │                         │
         ▼                         ▼
┌──────────────────────────────────────────────┐
│            Confluent Cloud (Kafka)            │
│  raw-logs          logstream.public.transactions │
│  (JSON Schema)     (Debezium CDC)             │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────┐
│              Flink SQL Processing             │
│  • Tumbling window aggregation (1-min)       │
│  • ML_DETECT_ANOMALIES (error rate spikes)   │
│  • AI_COMPLETE (Claude Haiku diagnosis)      │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────┐
│         ai_alerts_stream (Kafka topic)        │
│  Schema-registered + Data Contract           │
│  Tableflow → Iceberg materialization         │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────┐
│          Streamlit Dashboard                  │
│  Live alert feed + AI diagnosis cards        │
│  Metrics + Alerts by Service chart           │
└──────────────────────────────────────────────┘
```

## Confluent Cloud Features Used

| Feature | Usage |
|---------|-------|
| **Kafka Topics** | `raw-logs`, `ai_alerts_stream`, `logstream.public.transactions` |
| **Debezium PostgreSQL CDC** | Fully-managed connector capturing real-time DB changes |
| **Flink SQL** | Tumbling windows, `ML_DETECT_ANOMALIES`, `AI_COMPLETE` |
| **AI Model Inference** | Claude Haiku via `CREATE MODEL` + `AI_COMPLETE` |
| **Schema Registry** | JSON Schema on `raw-logs`, Avro on `ai_alerts_stream` |
| **Data Contracts** | Associated on governed topics |
| **Stream Lineage** | End-to-end visibility in Confluent Console |
| **Tableflow** | Iceberg materialization of `raw-logs` and `ai_alerts_stream` |

## Project Structure

```
logstream-ai/
├── scripts/
│   ├── log_producer.py      # VM-based log producer with --spike mode
│   └── dashboard.py         # Streamlit real-time dashboard (Avro-aware)
├── sql/
│   ├── 01_create_connection.sql
│   ├── 02_create_model.sql
│   ├── 03_windowed_anomaly.sql
│   ├── 04_ai_diagnosis.sql
│   └── 05_postgres_schema.sql
├── config/
│   └── confluent.env.example  # Template (real creds not committed)
├── docs/
│   └── architecture.md
├── .gitignore
└── README.md
```

## Setup

### Prerequisites

- Confluent Cloud account with Kafka cluster + Flink compute pool
- Python 3.6+ with `confluent-kafka` and `streamlit`
- PostgreSQL 15+ with logical replication enabled (for CDC)
- Anthropic API key (for AI diagnosis via Claude)

### 1. Configure Credentials

```bash
cp config/confluent.env.example config/confluent.env
# Edit with your actual credentials
chmod 600 config/confluent.env
```

### 2. Start Log Producer

```bash
cd scripts
python3 log_producer.py              # normal traffic
python3 log_producer.py --spike      # simulate incident (30s error burst)
python3 log_producer.py --rate 20    # higher throughput
```

### 3. Run Flink SQL Queries

Execute the SQL files in `sql/` directory in order (01 → 05) in the Confluent Cloud Flink SQL workspace.

### 4. Start Dashboard

```bash
ulimit -n 4096
cd scripts
python3 -m streamlit run dashboard.py
```

## Demo Flow

1. Show dashboard with baseline traffic
2. Click "Clear feed" → dashboard resets
3. Enable "Auto-refresh every 5s"
4. Run: `python3 log_producer.py --spike --spike-seconds 120`
5. Watch AI-diagnosed alerts flood the dashboard in real time
6. Expand any alert to read Claude's full root-cause analysis
7. Insert a failed transaction in PostgreSQL → CDC captures it live

## Tech Stack

- **Confluent Cloud** — Kafka, Flink SQL, Schema Registry, Connectors, Tableflow
- **Apache Flink** — Stream processing, ML_DETECT_ANOMALIES, AI_COMPLETE
- **Claude Haiku (Anthropic)** — Real-time root-cause diagnosis via AI Model Inference
- **Debezium** — PostgreSQL CDC connector
- **Streamlit** — Real-time dashboard
- **Python** — Log producer + dashboard
- **PostgreSQL 15** — Transaction database with logical replication

## Team

**Team Espada** — Confluent AI Day Indonesia 2026

## License

MIT
