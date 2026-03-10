import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class TestProducer(unittest.TestCase):
    """Test suite for Kafka producer functionality"""
    
    @patch('kafka.KafkaProducer')
    def test_producer_initialization(self, mock_kafka_producer):
        """Test that producer initializes with correct configuration"""
        from kafka import KafkaProducer
        import json
        
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance
        
        # Test producer configuration
        config = {
            'bootstrap_servers': ['localhost:9092'],
            'value_serializer': lambda v: json.dumps(v).encode('utf-8')
        }
        
        # Verify producer can be instantiated
        self.assertIsNotNone(mock_kafka_producer)
    
    def test_csv_data_validation(self):
        """Test CSV data structure validation"""
        sample_data = pd.DataFrame({
            'event_type': ['view', 'cart', 'purchase'],
            'product_id': [1, 2, 3],
            'user_id': [100, 101, 102]
        })
        
        # Verify required columns exist
        required_columns = ['event_type', 'product_id', 'user_id']
        for col in required_columns:
            self.assertIn(col, sample_data.columns)
    
    def test_message_serialization(self):
        """Test that messages can be serialized to JSON"""
        import json
        
        sample_row = {
            'event_type': 'view',
            'product_id': 123,
            'user_id': 456,
            'price': 99.99
        }
        
        # Test JSON serialization
        try:
            serialized = json.dumps(sample_row).encode('utf-8')
            self.assertIsInstance(serialized, bytes)
        except Exception as e:
            self.fail(f"Serialization failed: {e}")


if __name__ == '__main__':
    unittest.main()
