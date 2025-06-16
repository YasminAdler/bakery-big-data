"""
Kafka connector module for bakery analytics project.
This module provides helper functions to connect to Kafka with retry logic.
"""

import os
import time
import socket
import sys
import logging
import json
import subprocess

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_connector')

# Try to find and import KafkaProducer from various locations
try:
    # First try the custom implementation
    sys.path.append(os.path.join(os.path.dirname(__file__), 'producers'))
    from producers.kafka_producers import KafkaProducer
    logger.info("Using custom KafkaProducer from producers.kafka_producers")
except ImportError:
    try:
        # If that fails, try another import path
        from streaming.producers.kafka_producers import KafkaProducer
        logger.info("Using custom KafkaProducer from streaming.producers.kafka_producers")
    except ImportError:
        try:
            # Last resort - use standard kafka-python
            from kafka import KafkaProducer
            logger.info("Using KafkaProducer from kafka-python package")
        except ImportError:
            # If kafka-python is not installed, install it
            logger.warning("kafka-python not installed. Installing now...")
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'kafka-python'])
                from kafka import KafkaProducer
                logger.info("Successfully installed and imported kafka-python")
            except Exception as e:
                logger.error(f"Failed to install kafka-python: {e}")
                sys.exit(1)

def get_kafka_producer(max_retries=10, retry_interval=5):
    """Create a Kafka producer with retry logic"""
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    logger.info(f"Trying to connect to Kafka at {bootstrap_servers}")
    
    # First, check if Kafka host is reachable
    kafka_host = bootstrap_servers.split(':')[0]
    kafka_port = int(bootstrap_servers.split(':')[1])
    
    for retry in range(max_retries):
        try:
            # Test TCP connection to Kafka
            logger.info(f"Attempt {retry+1}/{max_retries}: Testing connection to {kafka_host}:{kafka_port}")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((kafka_host, kafka_port))
            sock.close()
            
            if result != 0:
                logger.warning(f"Cannot connect to {kafka_host}:{kafka_port}, retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
                continue
            
            # Try to create KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda v: json.dumps(v).encode('utf-8') if v is not None else None,
                request_timeout_ms=30000,  # Increase timeout
                api_version_auto_timeout_ms=30000,
                reconnect_backoff_ms=1000,
                reconnect_backoff_max_ms=10000
            )
            logger.info(f"✅ Successfully connected to Kafka at {bootstrap_servers}")
            return producer
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            if retry < max_retries - 1:
                logger.info(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
    
    logger.error(f"Failed to connect to Kafka after {max_retries} attempts")
    logger.info("Will run in simulation mode (messages will be logged instead of sent)")
    return KafkaProducer(bootstrap_servers=bootstrap_servers)  # This will use simulation mode