#!/usr/bin/env python3
"""
Equipment Metrics Producer for Kafka
Simulates IoT sensor data from bakery equipment
"""

import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
import os

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'equipment-metrics'

# Equipment definitions
EQUIPMENT = [
    {
        'id': 1,
        'name': 'Industrial Oven #1',
        'type': 'oven',
        'base_power': 15.0,  # kW
        'temp_range': (150, 250),  # Celsius
        'normal_status': 'running'
    },
    {
        'id': 2,
        'name': 'Industrial Oven #2',
        'type': 'oven',
        'base_power': 15.0,
        'temp_range': (150, 250),
        'normal_status': 'running'
    },
    {
        'id': 3,
        'name': 'Dough Mixer #1',
        'type': 'mixer',
        'base_power': 5.0,
        'rpm_range': (30, 120),
        'normal_status': 'running'
    },
    {
        'id': 4,
        'name': 'Dough Mixer #2',
        'type': 'mixer',
        'base_power': 5.0,
        'rpm_range': (30, 120),
        'normal_status': 'running'
    },
    {
        'id': 5,
        'name': 'Proofing Chamber #1',
        'type': 'proofer',
        'base_power': 3.0,
        'humidity_range': (65, 85),
        'temp_range': (25, 35),
        'normal_status': 'running'
    },
    {
        'id': 6,
        'name': 'Refrigerator #1',
        'type': 'refrigerator',
        'base_power': 2.5,
        'temp_range': (2, 8),
        'normal_status': 'running'
    },
    {
        'id': 7,
        'name': 'Display Case #1',
        'type': 'display_case',
        'base_power': 1.5,
        'temp_range': (4, 10),
        'normal_status': 'running'
    }
]


def generate_equipment_status(equipment):
    """Generate operational status for equipment"""
    # 95% of the time equipment runs normally
    if random.random() < 0.95:
        return equipment['normal_status']
    else:
        # Occasional different states
        states = ['maintenance', 'idle', 'starting', 'stopping', 'error']
        return random.choice(states)


def generate_power_consumption(equipment, status):
    """Generate power consumption based on equipment and status"""
    if status == 'idle':
        return round(equipment['base_power'] * 0.1, 2)
    elif status == 'maintenance' or status == 'error':
        return 0.0
    elif status == 'starting':
        return round(equipment['base_power'] * 1.5, 2)  # Power surge on startup
    else:  # running
        # Add some variance to simulate load changes
        variance = random.uniform(0.8, 1.2)
        return round(equipment['base_power'] * variance, 2)


def generate_equipment_metric(equipment):
    """Generate a single equipment metric event"""
    now = datetime.now()
    status = generate_equipment_status(equipment)
    
    metric = {
        'metric_id': f"METRIC_{uuid.uuid4().hex[:12]}",
        'equipment_id': equipment['id'],
        'event_time': now.isoformat(),
        'ingestion_time': datetime.now().isoformat(),
        'power_consumption': generate_power_consumption(equipment, status),
        'operational_status': status,
        'raw_payload': {}
    }
    
    # Add equipment-specific metrics
    if equipment['type'] == 'oven':
        metric['raw_payload']['temperature'] = random.uniform(*equipment['temp_range'])
        metric['raw_payload']['heating_element_status'] = 'on' if status == 'running' else 'off'
    
    elif equipment['type'] == 'mixer':
        metric['raw_payload']['rpm'] = random.uniform(*equipment['rpm_range']) if status == 'running' else 0
        metric['raw_payload']['motor_temperature'] = random.uniform(40, 80)
    
    elif equipment['type'] == 'proofer':
        metric['raw_payload']['humidity'] = random.uniform(*equipment['humidity_range'])
        metric['raw_payload']['temperature'] = random.uniform(*equipment['temp_range'])
    
    elif equipment['type'] in ['refrigerator', 'display_case']:
        metric['raw_payload']['temperature'] = random.uniform(*equipment['temp_range'])
        metric['raw_payload']['door_open'] = random.choice([True, False]) if status == 'running' else False
        metric['raw_payload']['compressor_status'] = 'on' if status == 'running' else 'off'
    
    metric['processing_status'] = 'pending'
    
    return metric


def main():
    """Main producer loop"""
    print(f"Starting Equipment Metrics Producer...")
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
            # Generate metrics for all equipment
            for equipment in EQUIPMENT:
                metric = generate_equipment_metric(equipment)
                
                # Send to Kafka
                producer.send(TOPIC_NAME, value=metric)
                event_count += 1
                
                print(f"[{event_count}] Sent metric: {equipment['name']}, "
                      f"Status: {metric['operational_status']}, "
                      f"Power: {metric['power_consumption']} kW")
            
            # Wait before next batch (simulate sensor reporting interval)
            time.sleep(30)  # Report every 30 seconds
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer stopped.")


if __name__ == "__main__":
    main() 