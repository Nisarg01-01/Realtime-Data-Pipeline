# Real-Time E-Commerce Data Pipeline

## 1) Introduction
This repository contains an end-to-end real-time analytics pipeline for e-commerce clickstream-style events. It streams raw events into Kafka, processes them with Spark Structured Streaming, persists them into PostgreSQL, builds session-level aggregates, and serves a Streamlit dashboard for live metrics and analysis.

The pipeline is designed to be easy to run locally and straightforward to extend (new metrics, new aggregates, different sinks, better data quality rules, and so on).

## 2) Architecture
High-level flow:

1. **Producer** reads a CSV file and publishes each row as a JSON message to Kafka.
2. **Spark consumer** reads the Kafka topic as a stream, parses/cleans records, and writes to PostgreSQL.
3. **Aggregation job** runs SQL to create a session summary table in PostgreSQL.
4. **Dashboard** queries PostgreSQL for live KPIs, charts, and session analytics.
5. **DB Agent (optional)** connects an LLM-backed SQL agent to the same database for natural-language questions.

ASCII diagram:

```
CSV (Data/2019-Oct.csv)
        |
        v
   producer.py
        |
        v
 Kafka topic: ecommerce_events
        |
        v
 consumer_spark.py (Spark Structured Streaming)
        |
        v
 PostgreSQL: ecommerce.events
        |
        +--> build_aggregates.py  -->  PostgreSQL: ecommerce.user_session_summary
        |
        +--> dashboard.py (Streamlit reads both tables)
        |
        +--> agent.py (LLM-backed SQL agent over the same DB)
```

## 3) Tech Stack
- **Python**: application code and orchestration scripts
- **Kafka + Zookeeper (Confluent images)**: event streaming backbone (local via Docker Compose)
- **PySpark / Spark Structured Streaming**: stream processing from Kafka
- **PostgreSQL**: storage for raw events and aggregated session analytics
- **SQLAlchemy + Pandas**: querying Postgres and building aggregates
- **Streamlit + Altair**: dashboard UI and charts
- **LangChain + Google GenAI (Gemini)**: natural-language SQL agent (optional)
- **GitHub Actions**: basic syntax checks in CI

## 4) Code Overview (What each file does)
- `docker-compose.yml`
  - Starts Zookeeper, Kafka, and PostgreSQL locally.
  - Exposes Kafka on `localhost:9092` and Postgres on `localhost:5432`.

- `producer.py`
  - Loads `Data/2019-Oct.csv` (first 100,000 rows) and publishes events to Kafka topic `ecommerce_events`.
  - Adds a small delay to simulate real-time streaming.

- `consumer_spark.py`
  - Spark Structured Streaming job that reads from Kafka topic `ecommerce_events`.
  - Parses JSON into a typed schema, performs simple cleaning, and writes each micro-batch to Postgres table `events`.

- `build_aggregates.py`
  - Runs an advanced SQL query over `events` to create session-level metrics:
    - session start/end
    - duration (minutes)
    - number of events
    - whether a session included cart/purchase events
  - Writes the results to Postgres table `user_session_summary` (replaces the table each run).

- `dashboard.py`
  - Streamlit dashboard that:
    - Shows live KPIs (views, carts, purchases)
    - Visualizes top brands/products by purchases
    - Shows session duration distribution from `user_session_summary`
    - Provides an optional “AI Business Analyst” input backed by `agent.py`

- `agent.py`
  - Creates a database-connected SQL agent using LangChain + Gemini.
  - Lets you ask natural-language questions that are translated into SQL against PostgreSQL.

- `requirements.txt`
  - Python dependency pin list for local installs.

- `.github/workflows/pyspark_test.yml`
  - CI workflow that runs basic Python syntax compilation on `consumer_spark.py` and `dashboard.py`.

- `.gitignore`
  - Ignores local virtual environments, caches, and large CSV files.

## 5) Environment Setup

### Prerequisites
- Docker Desktop (or compatible Docker engine)
- Python 3.11+ recommended
- Java 8/11+ (required by Spark)

### 1. Start Kafka + Postgres
From the repo root:

```bash
docker compose up -d
```

This starts:
- Kafka on `localhost:9092`
- Postgres on `localhost:5432` (DB: `ecommerce`, user: `user`, password: `password`)

### 2. Create a Python environment and install dependencies

```bash
python -m venv venv
# Windows PowerShell:
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Produce events into Kafka
Make sure the input dataset exists:
- `Data/2019-Oct.csv`

Then run:

```bash
python producer.py
```

### 4. Consume the stream with Spark and write to Postgres
Run the Spark streaming consumer:

```bash
python consumer_spark.py
```

If your Spark runtime needs explicit packages (Kafka source / Postgres JDBC driver), run with the appropriate Spark submit options for your setup.

### 5. Build session aggregates
In a separate terminal:

```bash
python build_aggregates.py
```

This creates/overwrites `user_session_summary` in Postgres.

### 6. Start the dashboard

```bash
streamlit run dashboard.py
```

Open the URL Streamlit prints (usually `http://localhost:8501`).

---

Nisarg Shah
