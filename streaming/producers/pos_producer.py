import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import os

fake = Faker()

class POSProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Sample data for realistic generation
        self.products = [
            {"id": 1, "name": "Croissant", "price": 3.50, "category": "Pastry"},
            {"id": 2, "name": "Baguette", "price": 2.00, "category": "Bread"},
            {"id": 3, "name": "Chocolate Cake", "price": 25.00, "category": "Cake"},
            {"id": 4, "name": "Apple Pie", "price": 18.00, "category": "Pie"},
            {"id": 5, "name": "Sourdough Bread", "price": 4.50, "category": "Bread"},
            {"id": 6, "name": "Blueberry Muffin", "price": 3.00, "category": "Muffin"},
            {"id": 7, "name": "Cinnamon Roll", "price": 4.00, "category": "Pastry"},
            {"id": 8, "name": "Cheesecake", "price": 30.00, "category": "Cake"}
        ]
        
        self.stores = [1, 2, 3, 4, 5]
        self.customers = [fake.uuid4() for _ in range(100)]
        
    def generate_sale_event(self):
        product = random.choice(self.products)
        quantity = random.randint(1, 5)
        
        event = {
            "event_id": fake.uuid4(),
            "event_time": datetime.now().isoformat(),
            "product_id": product["id"],
            "product_name": product["name"],
            "store_id": random.choice(self.stores),
            "quantity": quantity,
            "unit_price": product["price"],
            "total_price": product["price"] * quantity,
            "customer_id": random.choice(self.customers),
            "payment_method": random.choice(["cash", "credit", "debit", "mobile"]),
            "time_of_day": self.get_time_of_day(),
            "is_member": random.choice([True, False]),
            "discount_applied": random.choice([0, 5, 10, 15])
        }
        
        return event
    
    def get_time_of_day(self):
        hour = datetime.now().hour
        if 6 <= hour < 12:
            return "morning"
        elif 12 <= hour < 17:
            return "afternoon"
        elif 17 <= hour < 21:
            return "evening"
        else:
            return "night"
    
    def run(self):
        print("Starting POS Producer...")
        
        while True:
            try:
                # Generate and send event
                event = self.generate_sale_event()
                
                self.producer.send(
                    'bakery-pos-events',
                    key={"store_id": event["store_id"]},
                    value=event
                )
                
                print(f"Sent POS event: {event['event_id']} - {event['product_name']} x{event['quantity']}")
                
                # Variable rate to simulate business hours
                if 8 <= datetime.now().hour <= 20:
                    time.sleep(random.uniform(0.5, 2))  # Busy hours
                else:
                    time.sleep(random.uniform(5, 10))  # Quiet hours
                    
            except Exception as e:
                print(f"Error producing message: {e}")
                time.sleep(5)

if __name__ == "__main__":
    producer = POSProducer()
    producer.run()