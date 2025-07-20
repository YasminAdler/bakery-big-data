#!/usr/bin/env python3
"""
Sales Events Producer for Kafka
Generates real-time sales transactions for the bakery data pipeline
"""

import json
import random
import time
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import os

fake = Faker()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'sales-events'

# Business data
PRODUCTS = [
    {'id': 1, 'name': 'Croissant', 'price': 3.50, 'category': 'Pastry'},
    {'id': 2, 'name': 'Baguette', 'price': 2.80, 'category': 'Bread'},
    {'id': 3, 'name': 'Pain au Chocolat', 'price': 4.20, 'category': 'Pastry'},
    {'id': 4, 'name': 'Sourdough Loaf', 'price': 5.50, 'category': 'Bread'},
    {'id': 5, 'name': 'Blueberry Muffin', 'price': 3.20, 'category': 'Pastry'},
    {'id': 6, 'name': 'Whole Wheat Roll', 'price': 1.80, 'category': 'Bread'},
    {'id': 7, 'name': 'Apple Tart', 'price': 4.80, 'category': 'Pastry'},
    {'id': 8, 'name': 'Rye Bread', 'price': 4.20, 'category': 'Bread'},
    {'id': 9, 'name': 'Chocolate Eclair', 'price': 3.90, 'category': 'Pastry'},
    {'id': 10, 'name': 'French Bread', 'price': 2.50, 'category': 'Bread'}
]

STORES = [
    {'id': 1, 'location': 'Downtown', 'opening_hour': 6, 'closing_hour': 20},
    {'id': 2, 'location': 'Mall', 'opening_hour': 8, 'closing_hour': 22},
    {'id': 3, 'location': 'Airport', 'opening_hour': 5, 'closing_hour': 23},
    {'id': 4, 'location': 'Suburb North', 'opening_hour': 7, 'closing_hour': 19},
    {'id': 5, 'location': 'Suburb South', 'opening_hour': 7, 'closing_hour': 19}
]


def get_time_of_day(hour):
    """Determine time of day based on hour"""
    if 6 <= hour < 11:
        return 'morning'
    elif 11 <= hour < 15:
        return 'lunch'
    elif 15 <= hour < 18:
        return 'afternoon'
    else:
        return 'evening'


def generate_customer_id():
    """Generate a customer ID - mix of new and returning customers"""
    if random.random() < 0.3:  # 30% new customers
        return f"CUST_{uuid.uuid4().hex[:8]}"
    else:  # 70% returning customers
        return f"CUST_{random.randint(1000, 9999)}"


def generate_sales_event():
    """Generate a single sales event"""
    now = datetime.now()
    store = random.choice(STORES)
    
    # Generate event during store hours
    current_hour = now.hour
    if not (store['opening_hour'] <= current_hour < store['closing_hour']):
        # If outside hours, generate for a random time during open hours
        current_hour = random.randint(store['opening_hour'], store['closing_hour'] - 1)
        now = now.replace(hour=current_hour, minute=random.randint(0, 59))
    
    # Select products for this transaction (1-5 items)
    num_items = random.randint(1, 5)
    selected_products = random.sample(PRODUCTS, num_items)
    
    # Generate transaction
    event_id = f"EVT_{uuid.uuid4().hex[:12]}"
    customer_id = generate_customer_id()
    
    for product in selected_products:
        # Vary quantity based on product type
        if product['category'] == 'Bread':
            quantity = random.randint(1, 3)
        else:
            quantity = random.randint(1, 2)
        
        # Apply occasional discounts
        unit_price = product['price']
        if random.random() < 0.15:  # 15% chance of discount
            unit_price *= 0.9  # 10% discount
        
        event = {
            'event_id': f"{event_id}_{product['id']}",
            'event_time': now.isoformat(),
            'ingestion_time': datetime.now().isoformat(),
            'product_id': product['id'],
            'store_id': store['id'],
            'quantity': quantity,
            'unit_price': round(unit_price, 2),
            'customer_id': customer_id,
            'date': now.date().isoformat(),
            'time_of_day': get_time_of_day(current_hour),
            'processing_status': 'pending'
        }
        
        yield event


def main():
    """Main producer loop"""
    print(f"Starting Sales Events Producer...")
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
            # Generate sales events
            for event in generate_sales_event():
                # Send to Kafka
                producer.send(TOPIC_NAME, value=event)
                event_count += 1
                
                print(f"[{event_count}] Sent sales event: Store {event['store_id']}, "
                      f"Product {event['product_id']}, Qty {event['quantity']}, "
                      f"Customer {event['customer_id']}")
            
            # Wait before next batch (simulate real-time flow)
            wait_time = random.uniform(2, 10)  # 2-10 seconds between transactions
            time.sleep(wait_time)
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer stopped.")


if __name__ == "__main__":
    main() 