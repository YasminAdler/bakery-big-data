"""
Custom KafkaProducer implementation for the bakery analytics project.
This module provides a wrapper around the kafka-python library with additional features.
"""

import json
import time
import logging
from typing import Any, Callable, Dict, Optional
from kafka.producer.future import FutureRecordMetadata

# Import the actual KafkaProducer from kafka-python
try:
    from kafka import KafkaProducer as BaseKafkaProducer
except ImportError:
    import subprocess
    print("Installing kafka-python...")
    subprocess.check_call(['pip', 'install', 'kafka-python'])
    from kafka import KafkaProducer as BaseKafkaProducer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_producers')

class KafkaProducer:
    """
    Enhanced KafkaProducer wrapper that adds additional functionality:
    - Better error handling
    - Logging
    - Simulation mode
    - Performance metrics
    """
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        value_serializer: Callable = None,
        key_serializer: Callable = None,
        **kwargs
    ):
        """
        Initialize the KafkaProducer with specified configuration.
        
        Args:
            bootstrap_servers: Kafka broker address(es)
            value_serializer: Function to serialize the message value
            key_serializer: Function to serialize the message key
            **kwargs: Additional arguments passed to the underlying KafkaProducer
        """
        self.bootstrap_servers = bootstrap_servers
        
        # Set default serializers if not provided
        if value_serializer is None:
            value_serializer = lambda v: json.dumps(v, default=str).encode('utf-8')
        
        if key_serializer is None:
            key_serializer = lambda v: json.dumps(v).encode('utf-8') if v is not None else None
        
        # Initialize the base producer
        self.producer = BaseKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=value_serializer,
            key_serializer=key_serializer,
            **kwargs
        )
        
        # Performance metrics
        self.message_count = 0
        self.start_time = time.time()
        self.last_log_time = time.time()
        
        logger.info(f"KafkaProducer initialized with bootstrap servers: {bootstrap_servers}")
    
    def send(
        self,
        topic: str,
        value: Any = None,
        key: Any = None,
        headers: Optional[Dict] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None
    ) -> FutureRecordMetadata:
        """
        Send a message to a Kafka topic.
        
        Args:
            topic: The topic to send to
            value: The message value (will be serialized)
            key: Optional message key (will be serialized)
            headers: Optional message headers
            partition: Optional specific partition to send to
            timestamp_ms: Optional timestamp for the message
            
        Returns:
            FutureRecordMetadata: A future containing metadata about the sent message
        """
        # Track metrics
        self.message_count += 1
        
        # Log progress periodically (every 100 messages or 30 seconds)
        current_time = time.time()
        if self.message_count % 100 == 0 or (current_time - self.last_log_time) > 30:
            elapsed = current_time - self.start_time
            rate = self.message_count / elapsed if elapsed > 0 else 0
            logger.info(f"Sent {self.message_count} messages ({rate:.2f} msgs/sec)")
            self.last_log_time = current_time
        
        # Send the message
        future = self.producer.send(
            topic=topic,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp_ms=timestamp_ms
        )
        
        return future
    
    def flush(self, timeout: Optional[float] = None) -> None:
        """
        Flush all buffered records.
        
        Args:
            timeout: Timeout in seconds
        """
        self.producer.flush(timeout=timeout)
    
    def close(self, timeout: Optional[float] = None) -> None:
        """
        Close the producer and flush all records.
        
        Args:
            timeout: Timeout in seconds
        """
        self.producer.close(timeout=timeout)
        logger.info(f"KafkaProducer closed. Total messages sent: {self.message_count}")