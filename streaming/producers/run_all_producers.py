#!/usr/bin/env python3
"""
Run all Kafka producers concurrently
"""

import subprocess
import time
import os
import signal
import sys

# List of producer scripts
PRODUCERS = [
    'generate_sales_events.py',
    'generate_equipment_metrics.py',
    'generate_inventory_updates.py'
]

processes = []


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print('\nShutting down all producers...')
    for process in processes:
        process.terminate()
    sys.exit(0)


def main():
    """Start all producers"""
    signal.signal(signal.SIGINT, signal_handler)
    
    print("Starting all Kafka producers...")
    print(f"Kafka Bootstrap Servers: {os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
    print("-" * 50)
    
    # Start each producer in a separate process
    for producer_script in PRODUCERS:
        print(f"Starting {producer_script}...")
        process = subprocess.Popen(
            ['python', '-u', producer_script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        processes.append(process)
        time.sleep(2)  # Give each producer time to start
    
    print(f"\nAll {len(PRODUCERS)} producers started successfully!")
    print("Press Ctrl+C to stop all producers")
    print("-" * 50)
    
    # Monitor processes and print output
    try:
        while True:
            for i, process in enumerate(processes):
                # Check if process is still running
                if process.poll() is not None:
                    print(f"\nWARNING: Producer {PRODUCERS[i]} has stopped!")
                    # Restart the producer
                    print(f"Restarting {PRODUCERS[i]}...")
                    new_process = subprocess.Popen(
                        ['python', '-u', PRODUCERS[i]],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        universal_newlines=True,
                        bufsize=1
                    )
                    processes[i] = new_process
            
            time.sleep(5)  # Check every 5 seconds
            
    except KeyboardInterrupt:
        signal_handler(None, None)


if __name__ == "__main__":
    main() 