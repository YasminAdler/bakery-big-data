import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import os

class InventoryProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        
        self.products = list(range(1, 9))  # Product IDs 1-8
        self.stores = list(range(1, 6))    # Store IDs 1-5
        
        # Track inventory levels
        self.inventory_state = {}
        for store in self.stores:
            self.inventory_state[store] = {}
            for product in self.products:
                self.inventory_state[store][product] = {
                    "current_stock": random.randint(50, 200),
                    "last_update": datetime.now()
                }
        
        # Staff who report inventory
        self.staff = [
            "john.smith@bakery.com",
            "jane.doe@bakery.com", 
            "mike.wilson@bakery.com",
            "sarah.jones@bakery.com",
            "tom.brown@bakery.com"
        ]
    
    def generate_inventory_update(self):
        """Generate inventory update events that may arrive late"""
        store_id = random.choice(self.stores)
        product_id = random.choice(self.products)
        
        current_state = self.inventory_state[store_id][product_id]
        
        # Calculate inventory changes
        beginning_stock = current_state["current_stock"]
        
        # Simulate daily operations
        hours_since_last_update = random.randint(12, 36)
        
        # Sales based on time and randomness
        sold_quantity = random.randint(10, 50)
        
        # Restocking happens occasionally
        restocked_quantity = 0
        if beginning_stock < 30 or random.random() < 0.3:
            restocked_quantity = random.randint(50, 150)
        
        # Waste calculation (bakery products expire)
        waste_percentage = random.uniform(0.02, 0.08)  # 2-8% waste
        waste_quantity = int((beginning_stock + restocked_quantity) * waste_percentage)
        
        # Update state
        closing_stock = beginning_stock + restocked_quantity - sold_quantity - waste_quantity
        closing_stock = max(0, closing_stock)
        current_state["current_stock"] = closing_stock
        
        # Simulate late arrival (70% chance)
        if random.random() < 0.7:
            # Report time is in the past
            delay_hours = random.randint(1, 48)
            event_time = datetime.now() - timedelta(hours=delay_hours)
            ingestion_time = datetime.now()
            is_late = True
        else:
            # Real-time update
            event_time = datetime.now()
            ingestion_time = datetime.now()
            is_late = False
        
        event = {
            "update_id": f"inv_{store_id}_{product_id}_{int(time.time()*1000)}",
            "event_time": event_time.isoformat(),
            "ingestion_time": ingestion_time.isoformat(),
            "product_id": product_id,
            "store_id": store_id,
            "beginning_stock": beginning_stock,
            "restocked_quantity": restocked_quantity,
            "sold_quantity": sold_quantity,
            "waste_quantity": waste_quantity,
            "reported_by": random.choice(self.staff),
            "processing_status": "LATE_ARRIVAL" if is_late else "PENDING",
            "raw_payload": {
                "closing_stock": closing_stock,
                "waste_ratio": round(waste_quantity / (beginning_stock + restocked_quantity) if (beginning_stock + restocked_quantity) > 0 else 0, 4),
                "is_late_arrival": is_late,
                "delay_hours": delay_hours if is_late else 0,
                "update_reason": random.choice(["daily_count", "spot_check", "restock", "audit"]),
                "notes": "Low stock alert" if closing_stock < 20 else None
            }
        }
        
        current_state["last_update"] = datetime.now()
        
        return event
    
    def run(self):
        print("Starting Inventory Producer (with late arrivals)...")
        
        while True:
            try:
                # Generate inventory updates throughout the day
                # More updates during business hours
                hour = datetime.now().hour
                
                if 6 <= hour <= 20:  # Business hours
                    updates_to_send = random.randint(5, 10)
                else:  # After hours
                    updates_to_send = random.randint(1, 3)
                
                for _ in range(updates_to_send):
                    event = self.generate_inventory_update()
                    
                    self.producer.send(
                        'bakery-inventory-updates',
                        value=event
                    )
                    
                    if event["raw_payload"]["is_late_arrival"]:
                        print(f"📦 Late inventory update: Store {event['store_id']}, "
                              f"Product {event['product_id']} - {event['raw_payload']['delay_hours']}h late")
                    elif event["raw_payload"]["notes"]:
                        print(f"⚠️  {event['raw_payload']['notes']}: Store {event['store_id']}, "
                              f"Product {event['product_id']}")
                    
                    time.sleep(random.uniform(1, 5))
                
                # Wait before next batch
                time.sleep(random.uniform(300, 600))  # 5-10 minutes
                
            except Exception as e:
                print(f"Error producing inventory update: {e}")
                time.sleep(5)

if __name__ == "__main__":
    producer = InventoryProducer()
    producer.run()