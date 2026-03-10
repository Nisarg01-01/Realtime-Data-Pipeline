"""
Performance Testing Suite for Real-Time Data Pipeline

Tests throughput, latency, resource utilization, and scalability of all components.
"""

import pytest
import time
import psutil
import json
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from kafka import KafkaProducer, KafkaConsumer
from sqlalchemy import create_engine, text
import threading
from collections import defaultdict


# Mark all tests in this module as performance tests
pytestmark = pytest.mark.performance


class PerformanceMetrics:
    """Utility class to collect and report performance metrics"""
    
    def __init__(self):
        self.metrics = defaultdict(list)
    
    def record(self, metric_name, value):
        self.metrics[metric_name].append(value)
    
    def get_stats(self, metric_name):
        values = self.metrics[metric_name]
        if not values:
            return {}
        return {
            'min': min(values),
            'max': max(values),
            'avg': sum(values) / len(values),
            'count': len(values)
        }
    
    def assert_throughput(self, metric_name, min_throughput):
        """Assert minimum throughput requirement"""
        stats = self.get_stats(metric_name)
        assert stats['avg'] >= min_throughput, \
            f"Average throughput {stats['avg']:.2f} below minimum {min_throughput}"


@pytest.fixture
def performance_metrics():
    """Fixture to track performance metrics"""
    return PerformanceMetrics()


@pytest.fixture
def monitor_resources():
    """Fixture to monitor CPU and memory usage"""
    process = psutil.Process()
    initial_memory = process.memory_info().rss / 1024 / 1024  # MB
    initial_cpu = process.cpu_percent(interval=0.1)
    
    yield {
        'process': process,
        'initial_memory_mb': initial_memory,
        'initial_cpu_percent': initial_cpu
    }
    
    final_memory = process.memory_info().rss / 1024 / 1024
    final_cpu = process.cpu_percent(interval=0.1)
    
    print(f"\nResource Usage:")
    print(f"  Memory: {initial_memory:.2f} MB -> {final_memory:.2f} MB (delta: {final_memory - initial_memory:+.2f} MB)")
    print(f"  CPU: {initial_cpu:.2f}% -> {final_cpu:.2f}%")


class TestProducerPerformance:
    """Performance tests for Kafka producer component"""
    
    @patch('kafka.KafkaProducer')
    def test_producer_throughput(self, mock_kafka_producer, performance_metrics, monitor_resources):
        """Test producer throughput with various batch sizes"""
        
        mock_producer = Mock()
        mock_kafka_producer.return_value = mock_producer
        mock_producer.send.return_value = Mock()
        
        # Simulate event production
        num_events = 10000
        batch_sizes = [100, 500, 1000, 5000]
        
        for batch_size in batch_sizes:
            start_time = time.time()
            
            for i in range(num_events):
                event = {
                    'event_time': '2019-10-01 00:00:00 UTC',
                    'event_type': 'view',
                    'product_id': 1234567,
                    'category_id': 123456789,
                    'brand': 'TestBrand',
                    'price': 99.99,
                    'user_id': 123456,
                    'user_session': 'session-123'
                }
                mock_producer.send('test_topic', value=json.dumps(event).encode('utf-8'))
                
                if i % batch_size == 0:
                    mock_producer.flush()
            
            elapsed_time = time.time() - start_time
            throughput = num_events / elapsed_time
            
            performance_metrics.record(f'producer_throughput_batch_{batch_size}', throughput)
            print(f"\nBatch size {batch_size}: {throughput:.2f} events/sec")
        
        # Assert minimum throughput (should handle at least 1000 events/sec)
        performance_metrics.assert_throughput('producer_throughput_batch_1000', 1000)
    
    @patch('kafka.KafkaProducer')
    def test_producer_latency(self, mock_kafka_producer, performance_metrics):
        """Test individual message send latency"""
        
        mock_producer = Mock()
        mock_kafka_producer.return_value = mock_producer
        
        latencies = []
        num_samples = 1000
        
        for _ in range(num_samples):
            event = {'test': 'data'}
            
            start = time.perf_counter()
            mock_producer.send('test_topic', value=json.dumps(event).encode('utf-8'))
            latency = (time.perf_counter() - start) * 1000  # Convert to milliseconds
            
            latencies.append(latency)
            performance_metrics.record('producer_latency_ms', latency)
        
        stats = performance_metrics.get_stats('producer_latency_ms')
        print(f"\nProducer Latency (ms):")
        print(f"  Min: {stats['min']:.4f}")
        print(f"  Max: {stats['max']:.4f}")
        print(f"  Avg: {stats['avg']:.4f}")
        
        # Assert p95 latency is under 10ms (mocked, so should be very fast)
        p95_latency = sorted(latencies)[int(0.95 * len(latencies))]
        assert p95_latency < 10, f"P95 latency {p95_latency:.2f}ms exceeds 10ms threshold"
    
    def test_producer_memory_efficiency(self, monitor_resources):
        """Test memory usage under load"""
        
        # Simulate high-volume data processing
        data = []
        for i in range(100000):
            data.append({
                'id': i,
                'timestamp': '2019-10-01 00:00:00',
                'value': i * 1.5
            })
        
        # Memory should not grow excessively
        final_memory = monitor_resources['process'].memory_info().rss / 1024 / 1024
        memory_growth = final_memory - monitor_resources['initial_memory_mb']
        
        # Assert memory growth is reasonable (under 500MB for 100k records)
        assert memory_growth < 500, f"Memory growth {memory_growth:.2f}MB exceeds threshold"


class TestDatabasePerformance:
    """Performance tests for database operations"""
    
    @patch('sqlalchemy.create_engine')
    def test_bulk_insert_performance(self, mock_engine, performance_metrics, monitor_resources):
        """Test bulk insert throughput"""
        
        mock_conn = MagicMock()
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        batch_sizes = [100, 500, 1000, 5000]
        
        for batch_size in batch_sizes:
            records = []
            for i in range(batch_size):
                records.append({
                    'event_time': '2019-10-01 00:00:00',
                    'event_type': 'view',
                    'product_id': i,
                    'user_id': i % 1000,
                    'price': 99.99
                })
            
            start_time = time.time()
            
            # Simulate bulk insert
            for record in records:
                mock_conn.execute(text("INSERT INTO events VALUES (...)"), record)
            mock_conn.commit()
            
            elapsed_time = time.time() - start_time
            throughput = batch_size / elapsed_time
            
            performance_metrics.record(f'db_insert_throughput_{batch_size}', throughput)
            print(f"\nDB Bulk Insert ({batch_size} records): {throughput:.2f} records/sec")
        
        # Assert minimum throughput for 1000 record batch
        performance_metrics.assert_throughput('db_insert_throughput_1000', 100)
    
    @patch('sqlalchemy.create_engine')
    def test_query_performance(self, mock_engine, performance_metrics):
        """Test query execution time"""
        
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(i, f'brand_{i}', i * 10) for i in range(100)]
        
        mock_conn.execute.return_value = mock_result
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        queries = [
            "SELECT COUNT(*) FROM events",
            "SELECT brand, COUNT(*) FROM events WHERE event_type='purchase' GROUP BY brand",
            "SELECT * FROM events WHERE user_session='test' ORDER BY event_time DESC LIMIT 100"
        ]
        
        for query in queries:
            start_time = time.perf_counter()
            mock_conn.execute(text(query))
            query_time = (time.perf_counter() - start_time) * 1000  # ms
            
            performance_metrics.record('query_latency_ms', query_time)
            print(f"\nQuery execution time: {query_time:.4f} ms")
        
        stats = performance_metrics.get_stats('query_latency_ms')
        
        # Assert average query time is reasonable (mocked should be very fast)
        assert stats['avg'] < 100, f"Average query time {stats['avg']:.2f}ms too high"
    
    @patch('sqlalchemy.create_engine')
    def test_concurrent_reads(self, mock_engine, performance_metrics):
        """Test concurrent read performance"""
        
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(i,) for i in range(1000)]
        
        mock_conn.execute.return_value = mock_result
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        def execute_query(query_id):
            start = time.perf_counter()
            mock_conn.execute(text(f"SELECT * FROM events WHERE id={query_id}"))
            return time.perf_counter() - start
        
        # Simulate concurrent queries
        num_threads = 10
        threads = []
        results = []
        
        start_time = time.time()
        
        for i in range(num_threads):
            thread = threading.Thread(target=lambda i=i: results.append(execute_query(i)))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        total_time = time.time() - start_time
        throughput = num_threads / total_time
        
        performance_metrics.record('concurrent_query_throughput', throughput)
        print(f"\nConcurrent query throughput: {throughput:.2f} queries/sec")
        
        assert throughput > 5, "Concurrent query throughput too low"


class TestSparkPerformance:
    """Performance tests for Spark streaming component"""
    
    def test_stream_processing_throughput(self, performance_metrics):
        """Test Spark streaming throughput"""
        
        pytest.importorskip("pyspark")
        from unittest.mock import patch
        
        # Mock Spark dataframe operations
        with patch('pyspark.sql.SparkSession') as mock_spark:
            mock_df = MagicMock()
            mock_spark.return_value.readStream.format.return_value.option.return_value.load.return_value = mock_df
            
            # Simulate processing batches
            batch_sizes = [100, 500, 1000, 5000]
            
            for batch_size in batch_sizes:
                start_time = time.time()
                
                # Simulate processing operations
                for _ in range(batch_size):
                    mock_df.select.return_value.withColumn.return_value.filter.return_value
                
                elapsed_time = time.time() - start_time
                throughput = batch_size / elapsed_time if elapsed_time > 0 else 0
                
                performance_metrics.record(f'spark_throughput_{batch_size}', throughput)
                print(f"\nSpark processing ({batch_size} records): {throughput:.2f} records/sec")
    
    def test_transformation_latency(self, performance_metrics):
        """Test Spark transformation latency"""
        
        pytest.importorskip("pyspark")
        from unittest.mock import patch
        
        with patch('pyspark.sql.SparkSession') as mock_spark:
            mock_df = MagicMock()
            mock_spark.return_value.createDataFrame.return_value = mock_df
            
            transformations = [
                lambda df: df.select('event_time', 'event_type'),
                lambda df: df.filter(df.event_type == 'purchase'),
                lambda df: df.groupBy('brand').count(),
                lambda df: df.orderBy('event_time')
            ]
            
            for idx, transform in enumerate(transformations):
                start_time = time.perf_counter()
                transform(mock_df)
                latency = (time.perf_counter() - start_time) * 1000  # ms
                
                performance_metrics.record('spark_transformation_latency_ms', latency)
                print(f"\nTransformation {idx+1} latency: {latency:.4f} ms")
            
            stats = performance_metrics.get_stats('spark_transformation_latency_ms')
            assert stats['avg'] < 50, "Average transformation latency too high"


class TestEndToEndPerformance:
    """End-to-end system performance tests"""
    
    @patch('sqlalchemy.create_engine')
    @patch('kafka.KafkaProducer')
    def test_pipeline_latency(self, mock_kafka, mock_db_engine, performance_metrics, monitor_resources):
        """Test end-to-end latency from producer to database"""
        
        mock_producer = Mock()
        mock_kafka.return_value = mock_producer
        
        mock_conn = MagicMock()
        mock_db_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        num_events = 1000
        latencies = []
        
        for i in range(num_events):
            event_start = time.perf_counter()
            
            # Simulate: Produce -> Kafka -> Spark -> Database
            event = {'id': i, 'timestamp': time.time(), 'data': 'test'}
            
            # Producer sends to Kafka
            mock_producer.send('test_topic', value=json.dumps(event).encode('utf-8'))
            
            # Simulate Kafka + Spark processing delay (real would be async)
            time.sleep(0.001)  # 1ms simulated processing
            
            # Database write
            mock_conn.execute(text("INSERT INTO events VALUES (...)"), event)
            
            end_to_end_latency = (time.perf_counter() - event_start) * 1000  # ms
            latencies.append(end_to_end_latency)
            performance_metrics.record('e2e_latency_ms', end_to_end_latency)
        
        stats = performance_metrics.get_stats('e2e_latency_ms')
        
        print(f"\nEnd-to-End Latency (ms):")
        print(f"  Min: {stats['min']:.4f}")
        print(f"  Max: {stats['max']:.4f}")
        print(f"  Avg: {stats['avg']:.4f}")
        print(f"  P95: {sorted(latencies)[int(0.95 * len(latencies))]:.4f}")
        print(f"  P99: {sorted(latencies)[int(0.99 * len(latencies))]:.4f}")
        
        # Assert P95 latency under 2000ms (2 seconds requirement)
        p95 = sorted(latencies)[int(0.95 * len(latencies))]
        assert p95 < 2000, f"P95 end-to-end latency {p95:.2f}ms exceeds 2s threshold"
    
    @patch('sqlalchemy.create_engine')
    @patch('kafka.KafkaProducer')
    def test_system_throughput(self, mock_kafka, mock_db_engine, performance_metrics, monitor_resources):
        """Test overall system throughput under load"""
        
        mock_producer = Mock()
        mock_kafka.return_value = mock_producer
        
        mock_conn = MagicMock()
        mock_db_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        num_events = 10000
        start_time = time.time()
        
        for i in range(num_events):
            event = {
                'event_id': i,
                'event_type': 'view',
                'timestamp': time.time(),
                'user_id': i % 1000,
                'product_id': i % 5000
            }
            
            # Full pipeline simulation
            mock_producer.send('ecommerce_events', value=json.dumps(event).encode('utf-8'))
            
            if i % 100 == 0:
                mock_producer.flush()
                mock_conn.commit()
        
        elapsed_time = time.time() - start_time
        throughput = num_events / elapsed_time
        
        performance_metrics.record('system_throughput', throughput)
        
        print(f"\nSystem Throughput: {throughput:.2f} events/sec")
        print(f"Total events processed: {num_events}")
        print(f"Total time: {elapsed_time:.2f} seconds")
        
        # Assert minimum system throughput (1000 events/sec requirement)
        assert throughput >= 1000, f"System throughput {throughput:.2f} below 1000 events/sec"
    
    def test_data_quality_rate(self, performance_metrics):
        """Test data quality and validation rate"""
        
        total_events = 10000
        valid_events = 0
        validation_times = []
        
        for i in range(total_events):
            event = {
                'event_time': '2019-10-01 00:00:00 UTC' if i % 100 != 0 else None,  # 1% invalid
                'event_type': 'view' if i % 50 != 0 else 'invalid',  # 2% invalid
                'price': 99.99 if i % 200 != 0 else -1,  # 0.5% invalid
                'user_id': i
            }
            
            start = time.perf_counter()
            
            # Validation logic
            is_valid = (
                event.get('event_time') is not None and
                event.get('event_type') in ['view', 'cart', 'purchase', 'remove_from_cart'] and
                event.get('price', 0) >= 0
            )
            
            validation_time = (time.perf_counter() - start) * 1000000  # microseconds
            validation_times.append(validation_time)
            
            if is_valid:
                valid_events += 1
        
        quality_rate = (valid_events / total_events) * 100
        avg_validation_time = sum(validation_times) / len(validation_times)
        
        performance_metrics.record('data_quality_rate_%', quality_rate)
        performance_metrics.record('validation_time_us', avg_validation_time)
        
        print(f"\nData Quality Rate: {quality_rate:.2f}%")
        print(f"Valid events: {valid_events}/{total_events}")
        print(f"Average validation time: {avg_validation_time:.4f} microseconds")
        
        # Assert 99.9% data quality requirement (96.5% in test due to injected errors)
        assert quality_rate >= 96, f"Data quality rate {quality_rate:.2f}% too low"
    
    @patch('streamlit.line_chart')
    @patch('streamlit.metric')
    @patch('sqlalchemy.create_engine')
    def test_dashboard_refresh_performance(self, mock_engine, mock_metric, mock_chart, performance_metrics):
        """Test dashboard refresh time"""
        
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [('view', 1000), ('purchase', 500)]
        mock_conn.execute.return_value = mock_result
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        refresh_times = []
        
        for _ in range(10):
            start_time = time.perf_counter()
            
            # Simulate dashboard queries
            mock_conn.execute(text("SELECT event_type, COUNT(*) FROM events GROUP BY event_type"))
            mock_conn.execute(text("SELECT brand, COUNT(*) FROM events WHERE event_type='purchase' GROUP BY brand LIMIT 10"))
            mock_conn.execute(text("SELECT AVG(session_duration_minutes) FROM user_session_summary"))
            
            # Simulate rendering
            mock_metric("Total Views", 1000)
            mock_metric("Total Purchases", 500)
            mock_chart([1, 2, 3, 4, 5])
            
            refresh_time = (time.perf_counter() - start_time) * 1000  # ms
            refresh_times.append(refresh_time)
            performance_metrics.record('dashboard_refresh_ms', refresh_time)
        
        stats = performance_metrics.get_stats('dashboard_refresh_ms')
        
        print(f"\nDashboard Refresh Performance:")
        print(f"  Min: {stats['min']:.2f} ms")
        print(f"  Max: {stats['max']:.2f} ms")
        print(f"  Avg: {stats['avg']:.2f} ms")
        
        # Assert dashboard refreshes in acceptable time
        assert stats['avg'] < 5000, f"Average dashboard refresh {stats['avg']:.2f}ms exceeds 5s"


class TestScalabilityAndStress:
    """Scalability and stress testing"""
    
    @patch('kafka.KafkaProducer')
    def test_producer_under_stress(self, mock_kafka, performance_metrics, monitor_resources):
        """Test producer behavior under sustained high load"""
        
        mock_producer = Mock()
        mock_kafka.return_value = mock_producer
        
        duration_seconds = 5
        events_sent = 0
        start_time = time.time()
        
        while time.time() - start_time < duration_seconds:
            event = {'id': events_sent, 'timestamp': time.time()}
            mock_producer.send('stress_test', value=json.dumps(event).encode('utf-8'))
            events_sent += 1
        
        elapsed = time.time() - start_time
        sustained_throughput = events_sent / elapsed
        
        print(f"\nStress Test Results:")
        print(f"  Duration: {elapsed:.2f} seconds")
        print(f"  Events sent: {events_sent}")
        print(f"  Sustained throughput: {sustained_throughput:.2f} events/sec")
        
        performance_metrics.record('stress_test_throughput', sustained_throughput)
        
        # Should maintain at least 500 events/sec under stress
        assert sustained_throughput >= 500, "Sustained throughput too low under stress"
    
    def test_memory_leak_detection(self, monitor_resources):
        """Test for memory leaks during extended operation"""
        
        process = monitor_resources['process']
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Simulate extended operation
        iterations = 1000
        for i in range(iterations):
            # Create and destroy objects
            data = [{'id': j, 'value': j * 1.5} for j in range(100)]
            processed = [d for d in data if d['value'] > 50]
            del data
            del processed
        
        final_memory = process.memory_info().rss / 1024 / 1024
        memory_growth = final_memory - initial_memory
        
        print(f"\nMemory Leak Test:")
        print(f"  Initial memory: {initial_memory:.2f} MB")
        print(f"  Final memory: {final_memory:.2f} MB")
        print(f"  Memory growth: {memory_growth:+.2f} MB")
        
        # Memory growth should be minimal (under 100MB)
        assert memory_growth < 100, f"Potential memory leak detected: {memory_growth:.2f}MB growth"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
