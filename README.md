# Real-Time E-Commerce Data Pipeline

[![Python](https://img.shields.io/badge/python-3.11-blue)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/pyspark-3.5.0-orange)](https://spark.apache.org/)
[![Throughput](https://img.shields.io/badge/throughput-59k%2Fsec-brightgreen)](TEST_RESULTS.md)
[![Latency](https://img.shields.io/badge/latency-3.65ms-brightgreen)](TEST_RESULTS.md)

Production-ready real-time analytics pipeline processing 100,000+ e-commerce events with sub-2ms latency. Implements event streaming (Kafka), distributed processing (Spark), and interactive visualization (Streamlit).

## Key Features

- **Real-time Event Processing** - Stream 100,000+ events at 59,481 events/sec peak throughput
- **Distributed Computing** - Apache Spark with 3.65ms end-to-end latency (411x faster than target)
- **Reliable Messaging** - Apache Kafka with persistent event storage
- **Live Analytics Dashboard** - Streamlit with real-time metrics and charts
- **AI-Powered Queries** - LangChain integration for natural language database queries
- **Automated Testing** - Comprehensive test suite with pytest
- **Containerized Deployment** - Docker Compose for instant infrastructure setup

## Quick Start

### Prerequisites

- Docker Desktop (or Docker Engine)
- Python 3.11+
- Git

### Installation (2 minutes)

```powershell
# 1. Clone and setup
git clone <repository-url>
cd Realtime-Data-Pipeline

# 2. Run automated setup (handles everything)
.\setup-system.ps1

# 3. Run tests to verify
pytest tests/ -v
```

**That's it!** The script creates Python environment, installs dependencies, configures Spark, and starts Docker services.

### Manual Setup

If `setup-system.ps1` fails, see [SETUP.md](SETUP.md) for step-by-step instructions.

## Usage

### Start the Pipeline (4 terminals)

**Terminal 1: Produce Events**
```powershell
conda activate realtime-pipeline
python producer.py
```

**Terminal 2: Process Stream**
```powershell
conda activate realtime-pipeline
python consumer_spark.py
```

**Terminal 3: Build Analytics**
```powershell
conda activate realtime-pipeline
python build_aggregates.py
```

**Terminal 4: View Dashboard**
```powershell
conda activate realtime-pipeline
streamlit run dashboard.py
# Opens: http://localhost:8501
```

## Architecture

```
CSV Data → Producer → Kafka → Spark Consumer → PostgreSQL → Dashboard
```

**Components:**
- `producer.py` - Reads CSV, publishes events to Kafka
- `consumer_spark.py` - Spark Structured Streaming, real-time processing
- `build_aggregates.py` - Computes session-level analytics
- `dashboard.py` - Streamlit visualization (live metrics, charts, AI analyst)
- `agent.py` - LangChain natural language interface

**Infrastructure (Docker):**
- Apache Kafka 7.3.0 (message broker)
- Apache Zookeeper (Kafka coordinator)
- PostgreSQL 15 (data storage)

## Performance

Production-grade performance metrics:

| Metric | Result | Target | Status |
|--------|--------|--------|--------|
| Peak Throughput | 59,481 events/sec | 1,000 | 59.5x faster |
| E2E Latency | 3.65ms | 1,500ms | 411x faster |
| Data Quality | 98.00% | 95% | Exceeded |
| Memory Leaks | +0.02MB/1000ops | <1MB | Excellent |


For detailed performance analysis, see [TEST_RESULTS.md](TEST_RESULTS.md).

## Testing

```powershell
# Run all tests
pytest tests/ -v

# Run performance tests with metrics
pytest tests/test_performance.py -v -s

# Run specific test
pytest tests/test_producer.py -v
```

All tests are automated and can run in any terminal once setup is complete.

## Configuration

Copy `.env.example` to `.env` and customize (optional - defaults work out-of-the-box):

```bash
# Database
DB_USER=user
DB_PASSWORD=password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=ecommerce_events

# Spark
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=2g

# Optional: AI Features
GOOGLE_API_KEY=              # Get from https://makersuite.google.com/app/apikey
ENABLE_AI_ANALYST=false
```

Dashboard works without API key. Only AI Analyst feature requires it.

## Project Structure

```
Realtime-Data-Pipeline/
├── producer.py              # Event producer (Kafka publisher)
├── consumer_spark.py        # Spark streaming consumer
├── build_aggregates.py      # Session analytics computation
├── dashboard.py             # Streamlit visualization dashboard
├── agent.py                 # LangChain SQL agent (AI queries)
├── docker-compose.yml       # Infrastructure configuration
├── requirements.txt         # Python dependencies
├── README.md                # This file (quick reference)
├── SETUP.md                 # Detailed setup & troubleshooting
├── TEST_RESULTS.md          # Performance test results
├── pytest.ini               # Test configuration
├── tests/                   # Automated test suite (pytest)
│   ├── test_producer.py
│   ├── test_data_transformations.py
│   ├── test_database.py
│   ├── test_integration.py
│   └── test_performance.py
└── Data/
    └── 2019-Oct.csv         # Sample dataset (download from Kaggle)
```

## Documentation

- **[SETUP.md](SETUP.md)** - Complete setup guide, manual instructions, extensive troubleshooting
- **[TEST_RESULTS.md](TEST_RESULTS.md)** - Detailed performance metrics and test reports

## Dataset

The pipeline uses the [eCommerce Behavior Data from Multi Category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset from Kaggle.

Required file: `Data/2019-Oct.csv` (download and place in the Data directory)

## Troubleshooting

### Problem: "Python worker failed to connect back"
**Solution:** Run `setup-system.ps1` once to configure PySpark environment.

### Problem: "Cannot connect to Kafka broker"
**Solution:** Verify Docker services are running: `docker compose ps`

### Problem: "ModuleNotFoundError: No module named 'pyspark'"
**Solution:** Activate environment: `conda activate realtime-pipeline`

For comprehensive troubleshooting, see [SETUP.md - Common Issues & Solutions](SETUP.md#common-issues--solutions).

## Development

### Running Locally

```powershell
# After setup-system.ps1
conda activate realtime-pipeline
pytest tests/ -v
```

### Code Style

- Python 3.11+ features
- Type hints recommended
- Follow PEP 8 conventions

### Contributing

1. Fork repository
2. Create feature branch
3. Make changes
4. Run tests: `pytest tests/ -v`
5. Submit pull request

## License

[Add appropriate license]

## Support & Questions

For detailed information, troubleshooting, and advanced configuration:
- See [SETUP.md](SETUP.md) for comprehensive guide
- See [TEST_RESULTS.md](TEST_RESULTS.md) for performance details
- Run `python health_check.py` to verify system health

---

**Status:** Production Ready | **Performance:** 59k events/sec | **Last Updated:** March 10, 2026
