from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, trim, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def write_batch_to_postgres(batch_df, epoch_id):
    """
    This function is called for each micro-batch.
    It writes the DataFrame to a PostgreSQL table.
    """
    print(f"--- Writing batch {epoch_id} to PostgreSQL ---")
    
    # Define PostgreSQL connection properties
    postgres_url = "jdbc:postgresql://localhost:5432/ecommerce"
    postgres_properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    # Write the DataFrame to the 'events' table, appending new data
    batch_df.write.jdbc(
        url=postgres_url,
        table="events",
        mode="append",
        properties=postgres_properties
    )
    print(f"--- Batch {epoch_id} successfully written ---")

def main():
    spark = SparkSession.builder \
        .appName("ECommerceStreamConsumer") \
        .config("spark.driver.host", "127.0.0.1") \
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

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ecommerce_events") \
        .option("startingOffsets", "earliest") \
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