import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, trim, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dotenv import load_dotenv

load_dotenv()

def write_batch_to_postgres(batch_df, epoch_id):
    """
    This function is called for each micro-batch.
    It writes the DataFrame to a PostgreSQL table with optimizations.
    """
    if batch_df.isEmpty():
        print(f"--- Batch {epoch_id}: No data to write ---")
        return
        
    record_count = batch_df.count()
    print(f"--- Processing batch {epoch_id}: {record_count} records ---")
    
    # Define PostgreSQL connection properties from environment
    db_user = os.getenv("DB_USER", "user")
    db_password = os.getenv("DB_PASSWORD", "password")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "ecommerce")
    
    postgres_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    postgres_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver",
        # Performance optimizations
        "batchsize": "1000",  # Insert 1000 rows per batch
        "isolationLevel": "READ_UNCOMMITTED",  # Faster writes
        "reWriteBatchedInserts": "true"  # PostgreSQL optimization
    }

    # Write the DataFrame to the 'events' table, appending new data
    batch_df.write.jdbc(
        url=postgres_url,
        table="events",
        mode="append",
        properties=postgres_properties
    )
    print(f"--- Batch {epoch_id}: {record_count} records written successfully ---")

def main():
    spark = SparkSession.builder \
        .appName("ECommerceStreamConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define the schema
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_session", StringType(), True)
    ])

    # Read from Kafka with optimized configuration
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "ecommerce_events")
    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "10000") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse and clean the data
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    final_df = parsed_df.select("data.*")
    
    cleaned_df = final_df \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .filter(
            trim(col("category_code")).isNotNull() &
            (trim(col("category_code")) != "\"NaN\"") &
            trim(col("brand")).isNotNull() &
            (trim(col("brand")) != "\"NaN\"")
        )

    # Write the stream to PostgreSQL using the new function
    query = cleaned_df.writeStream \
        .foreachBatch(write_batch_to_postgres) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()