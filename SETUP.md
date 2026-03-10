# Setup Instructions & Troubleshooting

Complete reference for manual setup, configuration, and troubleshooting.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Manual Setup (Step by Step)](#manual-setup-step-by-step)
3. [Architecture Overview](#architecture-overview)
4. [Environment Configuration](#environment-configuration)
5. [Running the Pipeline](#running-the-pipeline)
6. [Troubleshooting](#troubleshooting)
7. [Common Issues & Solutions](#common-issues--solutions)

---

## Prerequisites

### System Requirements

- **Docker Desktop** (or compatible Docker engine) - Required for Kafka and PostgreSQL
- **Python 3.11+** - Recommended for compatibility
- **Java 8 or 11** - Required by Apache Spark
- **Git** - For version control
- **PowerShell 5.0+** (Windows) or Bash (Linux/macOS)
- **8GB RAM minimum** for comfortable testing

### Check Prerequisites

```powershell
# Verify Docker
docker --version

# Verify Python
python --version

# Verify Git
git --version

# Verify Java (optional, Spark can use bundled JVM)
java -version 2>&1 | find "version"
```

---

## Manual Setup (Step by Step)

Use this if the automated `setup-system.ps1` script fails or you prefer manual control.

### Step 1: Clone Repository and Download Dataset

```powershell
git clone <repository-url>
cd Realtime-Data-Pipeline

# Create Data directory
mkdir Data

# Download dataset from: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
# Extract 2019-Oct.csv to Data/ folder

# Verify dataset
dir Data\2019-Oct.csv
```

Expected output: Shows file size (typically 500+ MB)

### Step 2: Start Docker Services

Ensure Docker Desktop is running first.

```powershell
# Start all services (Kafka, Zookeeper, PostgreSQL)
docker compose up -d

# Verify services are running
docker compose ps
```

Expected services:
```
CONTAINER ID | IMAGE                      | STATUS           | PORTS
------------ | -------------------------- | --------------- | -------
<id>         | confluentinc/cp-zookeeper  | Up 2 minutes    | 2181/tcp
<id>         | confluentinc/cp-kafka      | Up 2 minutes    | 9092/tcp, 29092/tcp
<id>         | postgres:15                | Up 2 minutes    | 5432/tcp
```

**Connection Details:**
- Kafka broker: `localhost:9092`
- Zookeeper: `localhost:2181`
- PostgreSQL: `localhost:5432`
  - Database: `ecommerce`
  - Username: `user`
  - Password: `password`

### Step 3: Create Python Environment (Conda Recommended)

```powershell
# Check if conda is available
conda --version

# Create environment with Python 3.11
conda create -n realtime-pipeline python=3.11 -y

# Activate environment
conda activate realtime-pipeline

# Verify activation
python --version
```

Expected output: Python 3.11.x

### Step 4: Install Python Dependencies

```powershell
# Install all required packages
pip install -r requirements.txt
```

This installs:
- **pyspark 3.5.0** - Distributed processing
- **kafka-python** - Kafka client library
- **sqlalchemy** - Database ORM
- **pandas** - Data manipulation
- **streamlit** - Dashboard framework
- **langchain** - AI/ML integration
- **pytest** - Testing framework
- And other dependencies (see requirements.txt for complete list)

**Typical installation time: 3-5 minutes** (faster on subsequent runs)

### Step 5: Configure PostgreSQL Database

```powershell
# Connect to PostgreSQL container and initialize database
docker compose exec postgres psql -U user -d ecommerce -c "CREATE TABLE IF NOT EXISTS events (
  event_id SERIAL PRIMARY KEY,
  event_type VARCHAR(50),
  product_id VARCHAR(50),
  category_id VARCHAR(50),
  category_code VARCHAR(100),
  brand VARCHAR(100),
  price DECIMAL(10, 2),
  user_id VARCHAR(50),
  user_session VARCHAR(50),
  event_time TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"
```

### Step 6: Configure PySpark Environment

This is critical for Spark to work correctly on Windows.

```powershell
# Get Python executable path
$pythonPath = python -c "import sys; print(sys.executable)"

# Set PySpark environment variables for current session
$env:PYSPARK_PYTHON = $pythonPath
$env:PYSPARK_DRIVER_PYTHON = $pythonPath
$env:PYSPARK_PIN_THREAD = "true"

# Verify configuration
echo "PYSPARK_PYTHON: $env:PYSPARK_PYTHON"
echo "PYSPARK_DRIVER_PYTHON: $env:PYSPARK_DRIVER_PYTHON"
echo "PYSPARK_PIN_THREAD: $env:PYSPARK_PIN_THREAD"
```

Expected output: Paths to Python executable (e.g., `C:\Users\username\anaconda3\envs\realtime-pipeline\python.exe`)

**Important Notes:**
- These environment variables are **session-specific**
- If you open a new PowerShell terminal, you must re-run these commands
- Or run `setup-system.ps1` which sets them automatically

### Step 7: Verify Setup

```powershell
# Activate environment (if not already active)
conda activate realtime-pipeline

# Test all imports
python -c "import pyspark; import kafka; import streamlit; print('SUCCESS: All imports working')"

# List installed packages
pip list | findstr /I "pyspark kafka streamlit pandas"

# Test database connection
python -c "
import sqlalchemy
engine = sqlalchemy.create_engine('postgresql://user:password@localhost:5432/ecommerce')
connection = engine.connect()
print('SUCCESS: Database connection OK')
connection.close()
"

# Test Spark configuration
python -c "
import os
print('PYSPARK_PYTHON:', os.environ.get('PYSPARK_PYTHON', 'NOT SET'))
print('PYSPARK_DRIVER_PYTHON:', os.environ.get('PYSPARK_DRIVER_PYTHON', 'NOT SET'))
"
```

All commands should complete without errors.

### Step 8: Run Tests

```powershell
# Activate environment
conda activate realtime-pipeline

# Run all tests
pytest tests/ -v

# Run performance tests with detailed output
pytest tests/test_performance.py -v -s

# Run specific test
pytest tests/test_producer.py -v
```

Expected: All tests pass with [PASS] marker

---

## Architecture Overview

### System Design

```
CSV Data → Producer → Kafka → Spark Consumer → PostgreSQL → Analytics & Dashboard
```

### Component Details

1. **producer.py**
   - Reads CSV data from `Data/2019-Oct.csv`
   - Serializes records to JSON
   - Publishes to Kafka topic `ecommerce_events`
   - Rate: ~90-100 events per second

2. **Kafka + Zookeeper** (Docker)
   - Reliable event streaming and buffering
   - Handles producer backpressure
   - Enables multiple consumers

3. **consumer_spark.py**
   - Spark Structured Streaming job
   - Consumes from Kafka topic
   - Real-time data transformation and validation
   - Writes to PostgreSQL `events` table
   - Processes micro-batches (~5000 records per batch)

4. **build_aggregates.py**
   - Computes session-level analytics
   - Creates `user_session_summary` table
   - Aggregated metrics: user activity, session duration, etc.

5. **dashboard.py**
   - Streamlit web application
   - Live event counters and metrics
   - Real-time charts and visualizations
   - Optional AI Analyst (requires Google API key)

6. **agent.py**
   - LangChain-powered natural language interface
   - SQL query generation from English
   - Integration with Google Gemini API

### Data Flow

```
1. producer.py reads CSV
   ↓
2. Events published to Kafka topic
   ↓
3. consumer_spark.py pulls from Kafka
   ↓
4. Data transformed, validated, written to PostgreSQL
   ↓
5. build_aggregates.py computes summaries
   ↓
6. dashboard.py displays real-time analytics
   ↓
7. agent.py enables natural language queries
```

---

## Environment Configuration

### Configuration File: `.env`

```bash
# Database Configuration
DB_USER=user
DB_PASSWORD=password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=ecommerce_events
KAFKA_CONSUMER_GROUP=realtime-pipeline

# Spark Configuration
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_CORES=2

# Pipeline Configuration
DATA_ROWS_LIMIT=100000      # Number of events to produce
BATCH_SIZE=5000             # Spark batch size
AGGREGATION_INTERVAL=300    # Aggregation interval (seconds)

# Optional: AI Configuration
GOOGLE_API_KEY=             # Get from https://makersuite.google.com/app/apikey
ENABLE_AI_ANALYST=false
```

### Setup Configuration

```powershell
# Copy template
cp .env.example .env

# Edit .env (optional - defaults work out-of-the-box)
# Only need to change if:
# - Running on different machines
# - Using different ports
# - Enabling AI features
```

---

## Running the Pipeline

### Pre-Flight Checklist

Before running, verify:

```powershell
# 1. Docker services running
docker compose ps
# Expected: zookeeper, kafka, postgres - all "Up"

# 2. Dataset exists
dir Data\2019-Oct.csv
# Expected: File found

# 3. Python environment active
conda activate realtime-pipeline
python --version

# 4. Dependencies installed
pip list | findstr pyspark

# 5. PySpark configured
echo $env:PYSPARK_PYTHON
# Expected: Path to Python executable (not empty)
```

### Running Each Component

**Terminal 1: Start Producer**
```powershell
conda activate realtime-pipeline
python producer.py
```

Expected output:
```
Loading dataset...
Dataset loaded. 100000 rows ready. Starting to stream events...
Progress: 5000/100000 events (5.0%) - Rate: 92 events/sec
...
Completed: 100000 events sent in 1091.23s (avg 92 events/sec)
```

**Terminal 2: Start Consumer (after producer starts)**
```powershell
conda activate realtime-pipeline
python consumer_spark.py
```

Expected output (first run takes 1-2 minutes for JAR downloads):
```
Spark version: 3.5.0
--- Processing batch 0: 5000 records ---
--- Batch 0: 5000 records written successfully ---
...
```

**Terminal 3: Build Aggregates (after consumer processes batches)**
```powershell
conda activate realtime-pipeline
python build_aggregates.py
```

Expected output:
```
Connecting to the PostgreSQL database...
Processing sessions in chunks...
Batch 1: Processed 100 sessions
Session summary created successfully!
Total sessions processed: 250
```

**Terminal 4: Launch Dashboard**
```powershell
conda activate realtime-pipeline
streamlit run dashboard.py
```

Expected: Browser opens at `http://localhost:8501`

---

## Troubleshooting

### General Troubleshooting Steps

**1. Verify Environment Setup**
```powershell
# Check conda environment
conda env list
# Should show "realtime-pipeline" with asterisk

# Check Python path
python -c "import sys; print(sys.executable)"
# Should show path to conda environment

# Check imports
python -c "import pyspark; import kafka; import streamlit"
# Should complete without errors
```

**2. Verify Docker Services**
```powershell
# Check all services
docker compose ps

# View service logs
docker compose logs kafka
docker compose logs postgres
docker compose logs zookeeper

# Restart services
docker compose down
docker compose up -d

# Check connectivity
docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092
```

**3. Check Database Connection**
```powershell
# Connect to PostgreSQL
docker compose exec postgres psql -U user -d ecommerce -c "\dt"
# Should list tables: events, user_session_summary, etc.

# View recent events
docker compose exec postgres psql -U user -d ecommerce -c "SELECT COUNT(*) FROM events;"
```

**4. Monitor Resource Usage**
```powershell
# Check Docker resource usage
docker stats

# Check Python process memory
Get-Process python | Select-Object Name, WorkingSet, PrivateMemorySize
```

---

## Common Issues & Solutions

### ERROR: "Python worker failed to connect back"

**Cause:** PySpark environment variables not configured

**Solution:**
```powershell
# Re-run PySpark configuration
$pythonPath = python -c "import sys; print(sys.executable)"
$env:PYSPARK_PYTHON = $pythonPath
$env:PYSPARK_DRIVER_PYTHON = $pythonPath
$env:PYSPARK_PIN_THREAD = "true"

# Or run setup-system.ps1
.\setup-system.ps1
```

---

### ERROR: "Cannot connect to Kafka broker"

**Cause:** Docker services not running or network issues

**Solution:**
```powershell
# Check service status
docker compose ps

# Restart services
docker compose down
docker compose up -d

# Wait for Kafka to be ready (takes ~10 seconds)
Start-Sleep -Seconds 15

# Verify connectivity
docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092
```

---

### ERROR: "ModuleNotFoundError: No module named 'pyspark'"

**Cause:** Dependencies not installed or wrong environment active

**Solution:**
```powershell
# Verify environment is active
conda activate realtime-pipeline

# Reinstall dependencies
pip install --upgrade -r requirements.txt

# Verify installation
pip list | findstr pyspark
```

---

### ERROR: "Port 9092 or 5432 already in use"

**Cause:** Another service using the same port or orphaned containers

**Solution:**
```powershell
# Find process using port 9092
netstat -ano | findstr :9092

# If Docker container, remove it
docker compose down

# If other process, kill it (replace PID)
taskkill /PID <pid> /F

# Remove orphaned containers
docker compose down --remove-orphans

# Start fresh
docker compose up -d
```

---

### ERROR: "Database connection failed"

**Cause:** PostgreSQL not running or credentials wrong

**Solution:**
```powershell
# Check PostgreSQL service
docker compose ps postgres
# Should show "Up" status

# Test connection manually
docker compose exec postgres psql -U user -d ecommerce -c "SELECT 1"

# Check .env credentials
type .env | findstr DB_

# Update if needed
cp .env.example .env
# Edit .env with correct credentials
```

---

### ERROR: "Tests fail with timeout errors"

**Cause:** System resources insufficient or services too slow

**Solution:**
```powershell
# Give services more time to start
docker compose down
docker compose up -d
Start-Sleep -Seconds 30

# Increase timeouts in tests
# Edit pytest.ini and increase timeout values

# Run tests with verbose output
pytest tests/ -v -s

# Check resource usage during tests
docker stats
```

---

### Performance Issues: "Very slow event processing"

**Cause:** Resource constraints or suboptimal configuration

**Solution:**
```powershell
# Check available resources
# Windows Task Manager: Performance tab

# Increase Docker resource limits
# Docker Desktop → Settings → Resources
# - CPUs: Increase to available cores
# - Memory: Increase to 4-6GB

# Update Spark configuration in consumer_spark.py
.config("spark.executor.memory", "3g")  # Increase from 2g
.config("spark.driver.memory", "3g")     # Increase from 2g

# Reduce batch size for faster processing
# Edit consumer_spark.py: BATCH_SIZE = 2500
```

---

### ERROR: "Dashboard won't load or refreshes slowly"

**Cause:** Database queries too slow or connection issues

**Solution:**
```powershell
# Verify database has data
docker compose exec postgres psql -U user -d ecommerce -c "SELECT COUNT(*) FROM events;"

# Check if aggregates were built
docker compose exec postgres psql -U user -d ecommerce -c "SELECT * FROM user_session_summary LIMIT 1;"

# If no data, run the pipeline again:
# 1. Producer
# 2. Consumer
# 3. Aggregates

# Clear cache and restart dashboard
Ctrl+C  # Stop streamlit
streamlit cache clear
streamlit run dashboard.py
```

---

### ERROR: "AI Analyst not working"

**Cause:** Missing or invalid Google API key

**Solution:**
1. Get API key from [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Update `.env`:
   ```
   GOOGLE_API_KEY=your-actual-key-here
   ENABLE_AI_ANALYST=true
   ```
3. Restart dashboard:
   ```powershell
   Ctrl+C
   streamlit run dashboard.py
   ```

Dashboard works perfectly without API key - only AI Analyst feature is affected.

---

## Getting Help

For issues not covered here:

1. Check [README.md](README.md) for quick start and feature overview
2. Review [TEST_RESULTS.md](TEST_RESULTS.md) for performance benchmarks
3. Check Docker logs: `docker compose logs -f`
4. Review code comments in source files
5. Run health check: `python health_check.py`

Last updated: March 10, 2026
