"""
Kafka DNS Patch
--------------
This script patches the kafka-python library to fix DNS lookup issues when
running locally with a Kafka server configured for Docker.

Place this at the top of your run script:
    import kafka_dns_patch
"""

import logging
import socket
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_dns_patch')

# Original socket.getaddrinfo function
original_getaddrinfo = socket.getaddrinfo

# Host mapping dictionary
HOST_MAPPING = {
    'kafka': 'localhost',  # Redirect kafka to localhost
    'zookeeper': 'localhost',  # Redirect zookeeper to localhost
}

# Patched getaddrinfo function
def patched_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    """
    Patched version of socket.getaddrinfo that redirects Docker hostnames to localhost
    """
    # Check if the host is in our mapping
    if host in HOST_MAPPING:
        mapped_host = HOST_MAPPING[host]
        logger.info(f"[DNS Patch] Redirecting {host}:{port} to {mapped_host}:{port}")
        host = mapped_host
    
    # Call the original function
    return original_getaddrinfo(host, port, family, type, proto, flags)

# Apply the patch
socket.getaddrinfo = patched_getaddrinfo

# Also patch the KafkaProducer to fix advertised listeners
try:
    from kafka import KafkaProducer
    
    # Store the original constructor
    original_init = KafkaProducer.__init__
    
    # Create a patched constructor
    def patched_init(self, **configs):
        # Modify the bootstrap_servers parameter to ensure we're using localhost
        if 'bootstrap_servers' in configs:
            bootstrap_servers = configs['bootstrap_servers']
            if isinstance(bootstrap_servers, str):
                # Replace 'kafka:9092' with 'localhost:9092'
                if 'kafka:' in bootstrap_servers:
                    configs['bootstrap_servers'] = bootstrap_servers.replace('kafka:', 'localhost:')
                    logger.info(f"[KafkaProducer Patch] Redirected bootstrap_servers from {bootstrap_servers} to {configs['bootstrap_servers']}")
            elif isinstance(bootstrap_servers, list):
                # Replace in list
                new_servers = []
                for server in bootstrap_servers:
                    if 'kafka:' in server:
                        new_servers.append(server.replace('kafka:', 'localhost:'))
                        logger.info(f"[KafkaProducer Patch] Redirected bootstrap_servers from {server} to {server.replace('kafka:', 'localhost:')}")
                    else:
                        new_servers.append(server)
                configs['bootstrap_servers'] = new_servers
        
        # Call the original constructor
        original_init(self, **configs)
    
    # Apply the patch
    KafkaProducer.__init__ = patched_init
    logger.info("Patched KafkaProducer.__init__ to redirect kafka:9092 to localhost:9092")
    
except ImportError:
    logger.warning("Could not patch KafkaProducer - kafka-python not imported yet")

# Patch the KafkaConsumer as well
try:
    from kafka import KafkaConsumer
    
    # Store the original constructor
    original_consumer_init = KafkaConsumer.__init__
    
    # Create a patched constructor
    def patched_consumer_init(self, *topics, **configs):
        # Modify the bootstrap_servers parameter to ensure we're using localhost
        if 'bootstrap_servers' in configs:
            bootstrap_servers = configs['bootstrap_servers']
            if isinstance(bootstrap_servers, str):
                # Replace 'kafka:9092' with 'localhost:9092'
                if 'kafka:' in bootstrap_servers:
                    configs['bootstrap_servers'] = bootstrap_servers.replace('kafka:', 'localhost:')
                    logger.info(f"[KafkaConsumer Patch] Redirected bootstrap_servers from {bootstrap_servers} to {configs['bootstrap_servers']}")
            elif isinstance(bootstrap_servers, list):
                # Replace in list
                new_servers = []
                for server in bootstrap_servers:
                    if 'kafka:' in server:
                        new_servers.append(server.replace('kafka:', 'localhost:'))
                        logger.info(f"[KafkaConsumer Patch] Redirected bootstrap_servers from {server} to {server.replace('kafka:', 'localhost:')}")
                    else:
                        new_servers.append(server)
                configs['bootstrap_servers'] = new_servers
        
        # Call the original constructor
        original_consumer_init(self, *topics, **configs)
    
    # Apply the patch
    KafkaConsumer.__init__ = patched_consumer_init
    logger.info("Patched KafkaConsumer.__init__ to redirect kafka:9092 to localhost:9092")
    
except ImportError:
    logger.warning("Could not patch KafkaConsumer - kafka-python not imported yet")

# Patch the BrokerConnection class to handle DNS failures
try:
    from kafka.conn import BrokerConnection
    
    # Store the original connect method
    original_connect = BrokerConnection.connect
    
    # Create a patched connect method
    def patched_connect(self, *args, **kwargs):
        # If the host is 'kafka', redirect to 'localhost'
        if self.host in HOST_MAPPING:
            logger.info(f"[BrokerConnection Patch] Redirecting {self.host}:{self.port} to {HOST_MAPPING[self.host]}:{self.port}")
            self.host = HOST_MAPPING[self.host]
        
        # Call the original method
        return original_connect(self, *args, **kwargs)
    
    # Apply the patch
    BrokerConnection.connect = patched_connect
    logger.info("Patched BrokerConnection.connect to redirect kafka:9092 to localhost:9092")
    
except ImportError:
    logger.warning("Could not patch BrokerConnection - kafka.conn not imported yet")

# Create an interceptor for future imports
class KafkaPatchFinder:
    def find_spec(self, fullname, path, target=None):
        if fullname == 'kafka' or fullname.startswith('kafka.'):
            # Let the original import happen
            return None
        
        return None
    
    def load_module(self, fullname):
        if fullname == 'kafka':
            # Let the original import happen, then patch
            module = __import__('kafka')
            # Patch will be applied when the module is imported
            return module
        
        return None

# Register the finder
sys.meta_path.insert(0, KafkaPatchFinder())

logger.info("Kafka DNS Patch has been applied")