#!/usr/bin/env python3
"""
Inventory Updates Producer for Kafka
Generates inventory updates with late arrival simulation (up to 48 hours)
"""

import json
import random
import time
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
import os

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'inventory-updates'

# Product and store definitions (matching sales events)
PRODUCTS = list(range(1, 11))  # Product IDs 1-10
STORES = list(range(1, 6))     # Store IDs 1-5

# Staff who report inventory
STAFF_MEMBERS = [
    'John Smith', 'Emma Johnson', 'Michael Brown', 'Sarah Davis',
    'David Wilson', 'Lisa Anderson', 'James Taylor', 'Mary Thomas'
]


def simulate_late_arrival():
    """Simulate late arrival by returning a past event time"""
    # 70% arrive on time, 20% arrive within 24 hours, 10% arrive 24-48 hours late
    rand = random.random()
    
    if rand < 0.7:
        # On time (within last hour)
        delay_minutes = random.randint(0, 60)
        return datetime.now() - timedelta(minutes=delay_minutes)
    elif rand < 0.9:
        # 1-24 hours late
        delay_hours = random.uniform(1, 24)
        return datetime.now() - timedelta(hours=delay_hours)
    else:
        # 24-48 hours late
        delay_hours = random.uniform(24, 48)
        return datetime.now() - timedelta(hours=delay_hours)


def calculate_inventory_quantities(product_id):
    """Calculate realistic inventory quantities based on product type"""
    # Bread products (IDs 2, 4, 6, 8, 10) have higher volumes
    is_bread = product_id % 2 == 0
    
    if is_bread:
        beginning_stock = random.randint(50, 150)
        restocked = random.randint(30, 100)
        sold = random.randint(40, 120)
    else:  # Pastries
        beginning_stock = random.randint(20, 80)
        restocked = random.randint(20, 60)
        sold = random.randint(15, 70)
    
    # Calculate waste (higher for products with shorter shelf life)
    waste_percentage = random.uniform(0.02, 0.15)  # 2-15% waste
    waste = int((beginning_stock + restocked) * waste_percentage)
    
    return {
        'beginning_stock': beginning_stock,
        'restocked_quantity': restocked,
        'sold_quantity': sold,
        'waste_quantity': waste
    }


def generate_inventory_update():
    """Generate a single inventory update event"""
    # Event time (potentially in the past for late arrivals)
    event_time = simulate_late_arrival()
    ingestion_time = datetime.now()
    
    # Select random product and store
    product_id = random.choice(PRODUCTS)
    store_id = random.choice(STORES)
    
    # Calculate quantities
    quantities = calculate_inventory_quantities(product_id)
    
    # Create update event
    update = {
        'update_id': f"INV_{uuid.uuid4().hex[:12]}",
        'event_time': event_time.isoformat(),
        'ingestion_time': ingestion_time.isoformat(),
        'product_id': product_id,
        'store_id': store_id,
        'beginning_stock': quantities['beginning_stock'],
        'restocked_quantity': quantities['restocked_quantity'],
        'sold_quantity': quantities['sold_quantity'],
        'waste_quantity': quantities['waste_quantity'],
        'reported_by': random.choice(STAFF_MEMBERS),
        'processing_status': 'pending',
        'late_arrival_hours': round((ingestion_time - event_time).total_seconds() / 3600, 2)
    }
    
    return update


def main():
    """Main producer loop"""
    print(f"Starting Inventory Updates Producer...")
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {TOPIC_NAME}")
    
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )
    
    try:
        event_count = 0
        while True:
            # Generate 1-5 inventory updates per batch
            batch_size = random.randint(1, 5)
            
            for _ in range(batch_size):
                update = generate_inventory_update()
                
                # Send to Kafka
                producer.send(TOPIC_NAME, value=update)
                event_count += 1
                
                # Log the update
                late_status = "ON TIME" if update['late_arrival_hours'] < 1 else f"LATE by {update['late_arrival_hours']:.1f} hours"
                print(f"[{event_count}] Sent inventory update: Store {update['store_id']}, "
                      f"Product {update['product_id']}, "
                      f"Reported by {update['reported_by']} - {late_status}")
            
            # Wait before next batch (simulate periodic inventory reporting)
            wait_time = random.uniform(60, 300)  # 1-5 minutes between batches
            time.sleep(wait_time)
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer stopped.")


if __name__ == "__main__":
    main() 