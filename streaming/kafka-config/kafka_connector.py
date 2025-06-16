# kafka_connector.py
import os
import time
import socket
from kafka import KafkaProducer
import json

def get_kafka_producer(max_retries=10, retry_interval=5):
    """Create a Kafka producer with retry logic"""
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    print(f"Trying to connect to Kafka at {bootstrap_servers}")
    
    # First, check if Kafka host is reachable
    kafka_host = bootstrap_servers.split(':')[0]
    kafka_port = int(bootstrap_servers.split(':')[1])
    
    for retry in range(max_retries):
        try:
            # Test TCP connection to Kafka
            print(f"Attempt {retry+1}/{max_retries}: Testing connection to {kafka_host}:{kafka_port}")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((kafka_host, kafka_port))
            sock.close()
            
            if result != 0:
                print(f"Cannot connect to {kafka_host}:{kafka_port}, retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
                continue
            
            # Try to create KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=30000,  # Increase timeout
                api_version_auto_timeout_ms=30000,
                reconnect_backoff_ms=1000,
                reconnect_backoff_max_ms=10000
            )
            print(f"✅ Successfully connected to Kafka at {bootstrap_servers}")
            return producer
            
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")
            if retry < max_retries - 1:
                print(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
    
    raise Exception(f"Failed to connect to Kafka after {max_retries} attempts")