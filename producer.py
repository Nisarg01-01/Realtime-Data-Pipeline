import os
import pandas as pd
from kafka import KafkaProducer
import json
import time
from dotenv import load_dotenv

load_dotenv()

# Initialize Kafka Producer with optimized configuration for performance
kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(',')
producer = KafkaProducer(
    bootstrap_servers=kafka_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    # Performance optimizations
    batch_size=32768,  # 32KB batch size for better throughput
    linger_ms=10,  # Wait up to 10ms to batch messages
    compression_type='snappy',  # Compress messages for better network utilization
    buffer_memory=67108864,  # 64MB buffer
    max_in_flight_requests_per_connection=5  # Allow pipelining for better throughput
)

# Load the dataset with configurable parameters
data_file_path = os.getenv("DATA_FILE_PATH", "Data/2019-Oct.csv")
data_rows_limit = int(os.getenv("DATA_ROWS_LIMIT", "100000"))

print("Loading dataset...")
df = pd.read_csv(data_file_path, nrows=data_rows_limit)
print(f"Dataset loaded. {len(df)} rows ready. Starting to stream events...")

# Define the topic from environment
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")

# Optimize: Convert dataframe to dict records once (faster than iterrows)
records = df.to_dict('records')
total_records = len(records)
producer_delay = float(os.getenv("PRODUCER_DELAY", "0.01"))

# Performance: Show progress every N events instead of every event
progress_interval = max(1, total_records // 20)  # Show 20 progress updates

start_time = time.time()

# Loop through records and send messages
for index, message in enumerate(records):
    producer.send(KAFKA_TOPIC, value=message)
    
    # Progress indicator (minimal logging for performance)
    if (index + 1) % progress_interval == 0 or (index + 1) == total_records:
        elapsed = time.time() - start_time
        rate = (index + 1) / elapsed if elapsed > 0 else 0
        print(f"Progress: {index + 1}/{total_records} events ({(index+1)/total_records*100:.1f}%) - Rate: {rate:.0f} events/sec")
    
    # Simulate real-time stream with configurable delay
    if producer_delay > 0:
        time.sleep(producer_delay)

producer.flush()
elapsed_total = time.time() - start_time
avg_rate = total_records / elapsed_total if elapsed_total > 0 else 0
print(f"\nCompleted: {total_records} events sent in {elapsed_total:.2f}s (avg {avg_rate:.0f} events/sec)")