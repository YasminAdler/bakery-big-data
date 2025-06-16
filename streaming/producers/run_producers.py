"""
Run All Producers with Kafka DNS Fix
-----------------------------------
This script runs all producers with the Kafka DNS fix applied.

Usage:
    python run_producers_with_fix.py
"""

# Apply fixes BEFORE any other imports
import os
import sys
import kafka_dns_patch as kafka_dns_patch


# Fix environment variables
os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'localhost:9092'

# Import the DNS patch to fix Kafka hostname resolution
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

try:
    import streaming.producers.kafka_dns_patch as kafka_dns_patch
except ImportError:
    print("kafka_dns_patch.py not found in the current directory")
    print("Make sure to copy it to:", script_dir)
    sys.exit(1)

# Now import and run the original run_all_producers script
try:
    import run_all_producers
    # The script will run automatically through its if __name__ == "__main__" block
except ImportError:
    print("run_all_producers.py not found in the current directory")
    print("Make sure to copy it to:", script_dir)
    
    # Fallback to directly running the producers
    import time
    import threading
    import logging
    import importlib
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger('producer_runner')
    
    # Define the producers to run
    producers = [
        ('pos', 'pos_producer', 'POSProducer'),
        ('inventory', 'inventory_producer', 'InventoryProducer'),
        ('iot', 'iot_producer', 'IoTProducer')
    ]
    
    # Function to run a producer
    def run_producer(producer_type, module_name, class_name):
        logger.info(f"Starting {producer_type} producer ({class_name})...")
        try:
            # Import the module
            module = importlib.import_module(module_name)
            
            # Get the producer class
            if not hasattr(module, class_name):
                logger.error(f"Could not find class {class_name} in module {module_name}")
                return
            
            producer_class = getattr(module, class_name)
            
            # Create and run the producer
            producer = producer_class()
            producer.run()
        except Exception as e:
            logger.error(f"Error running {producer_type} producer: {e}", exc_info=True)
    
    # Run all producers in separate threads
    logger.info(f"Running all producers in parallel")
    threads = []
    
    for producer_type, module_name, class_name in producers:
        thread = threading.Thread(
            target=run_producer, 
            args=(producer_type, module_name, class_name),
            name=f"{producer_type}_thread"
        )
        thread.daemon = True
        threads.append(thread)
        thread.start()
        logger.info(f"Started {producer_type} producer thread")
    
    # Keep the main thread alive
    try:
        while True:
            # Check if all threads are still alive
            alive_threads = [t for t in threads if t.is_alive()]
            if not alive_threads:
                logger.warning("All producer threads have stopped!")
                break
            
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        sys.exit(0)