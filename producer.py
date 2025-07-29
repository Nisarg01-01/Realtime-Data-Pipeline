import pandas as pd
from kafka import KafkaProducer
import json
import time

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load the dataset
print("Loading dataset...")
df = pd.read_csv('Data/2019-Oct.csv', nrows=100000) # Using 100k rows for speed
print("Dataset loaded. Starting to stream events...")

# Define the topic
KAFKA_TOPIC = 'ecommerce_events'

# Loop through the dataframe and send each row as a message
for index, row in df.iterrows():
    message = row.to_dict()
    producer.send(KAFKA_TOPIC, value=message)
    print(f"Sent event: {message['event_type']} for product {message['product_id']}")

    # Simulate a real-time stream with a small delay
    time.sleep(0.01) 

producer.flush()
print("All events have been sent.")