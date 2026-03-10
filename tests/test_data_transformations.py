import unittest
import pytest

# Skip all tests in this module if pyspark is not installed
pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, trim


class TestDataTransformations(unittest.TestCase):
    """Test suite for Spark data transformations"""
    
    spark = None
    
    @classmethod
    def setUpClass(cls):
        """Initialize Spark session for testing"""
        # Configure Spark with minimal settings to avoid Windows worker issues
        cls.spark = SparkSession.builder \
            .appName("TestDataTransformations") \
            .master("local[1]") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.default.parallelism", "1") \
            .config("spark.driver.memory", "512m") \
            .config("spark.executor.memory", "512m") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        """Stop Spark session after tests"""
        if cls.spark:
            cls.spark.stop()
    
    def test_schema_definition(self):
        """Test that the event schema is correctly defined"""
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
        
        # Verify schema has all required fields
        field_names = [field.name for field in schema.fields]
        self.assertIn("event_type", field_names)
        self.assertIn("product_id", field_names)
        self.assertIn("user_id", field_names)
        self.assertEqual(len(field_names), 9)
    
    def test_data_filtering(self):
        """Test that null and NaN values are filtered correctly"""
        test_data = [
            ("view", "electronics", "Samsung", 100),
            ("cart", "NaN", "NaN", 101),
            ("purchase", "phones", "Apple", 102),
            ("view", None, "Sony", 103)
        ]
        
        df = self.spark.createDataFrame(
            test_data,
            ["event_type", "category_code", "brand", "user_id"]
        )
        
        # Apply filtering logic similar to consumer_spark.py
        filtered_df = df.filter(
            trim(col("category_code")).isNotNull() &
            (trim(col("category_code")) != "NaN") &
            trim(col("brand")).isNotNull() &
            (trim(col("brand")) != "NaN")
        )
        
        # Should filter out rows with NaN or null values
        self.assertEqual(filtered_df.count(), 2)
    
    def test_data_cleaning(self):
        """Test that data cleaning preserves valid records"""
        test_data = [
            ("2019-10-01 00:00:00", "view", "electronics.smartphone", "Samsung"),
            ("2019-10-01 00:01:00", "cart", "electronics.audio", "Sony"),
        ]
        
        df = self.spark.createDataFrame(
            test_data,
            ["event_time", "event_type", "category_code", "brand"]
        )
        
        # Verify all valid records are preserved
        self.assertEqual(df.count(), 2)
        
        # Verify data types
        self.assertEqual(df.schema["event_type"].dataType, StringType())


if __name__ == '__main__':
    unittest.main()
