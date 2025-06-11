import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import os
import numpy as np

fake = Faker()

class POSProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Enhanced product data matching your schema
        self.products = [
            {"id": 1, "name": "Croissant", "price": 3.50, "category": "Pastry", "popularity": 0.9, "shelf_life_hours": 24},
            {"id": 2, "name": "Baguette", "price": 2.00, "category": "Bread", "popularity": 0.8, "shelf_life_hours": 48},
            {"id": 3, "name": "Chocolate Cake", "price": 25.00, "category": "Cake", "popularity": 0.6, "shelf_life_hours": 72},
            {"id": 4, "name": "Apple Pie", "price": 18.00, "category": "Pie", "popularity": 0.7, "shelf_life_hours": 48},
            {"id": 5, "name": "Sourdough Bread", "price": 4.50, "category": "Bread", "popularity": 0.75, "shelf_life_hours": 96},
            {"id": 6, "name": "Blueberry Muffin", "price": 3.00, "category": "Muffin", "popularity": 0.65, "shelf_life_hours": 48},
            {"id": 7, "name": "Cinnamon Roll", "price": 4.00, "category": "Pastry", "popularity": 0.85, "shelf_life_hours": 24},
            {"id": 8, "name": "Cheesecake", "price": 30.00, "category": "Cake", "popularity": 0.5, "shelf_life_hours": 96}
        ]
        
        # Store profiles with different characteristics
        self.stores = [
            {"id": 1, "type": "Downtown", "traffic_multiplier": 1.5, "peak_hours": [7, 9, 12, 13, 17, 19]},
            {"id": 2, "type": "Mall", "traffic_multiplier": 1.2, "peak_hours": [11, 12, 13, 14, 15, 16, 17, 18]},
            {"id": 3, "type": "Airport", "traffic_multiplier": 1.0, "peak_hours": [5, 6, 7, 8, 14, 15, 16, 20, 21]},
            {"id": 4, "type": "Suburban", "traffic_multiplier": 0.8, "peak_hours": [8, 9, 17, 18, 19]},
            {"id": 5, "type": "University", "traffic_multiplier": 0.9, "peak_hours": [8, 12, 13, 14, 18]}
        ]
        
        # Customer segments
        self.customer_segments = {
            "regular": {"count": 100, "loyalty_rate": 0.8, "avg_spend_multiplier": 1.0},
            "student": {"count": 50, "loyalty_rate": 0.6, "avg_spend_multiplier": 0.7},
            "tourist": {"count": 30, "loyalty_rate": 0.1, "avg_spend_multiplier": 1.3},
            "business": {"count": 40, "loyalty_rate": 0.5, "avg_spend_multiplier": 1.5}
        }
        
        # Generate customers with segments
        self.customers = []
        for segment, props in self.customer_segments.items():
            for _ in range(props["count"]):
                self.customers.append({
                    "id": fake.uuid4(),
                    "segment": segment,
                    "is_member": random.random() < props["loyalty_rate"],
                    "spend_multiplier": props["avg_spend_multiplier"]
                })
        
        # Seasonal patterns
        self.seasonal_patterns = {
            "winter": {"bread": 1.2, "pastry": 1.3, "cake": 0.9},
            "spring": {"bread": 1.0, "pastry": 1.1, "cake": 1.2},
            "summer": {"bread": 0.8, "pastry": 0.9, "cake": 1.3},
            "fall": {"bread": 1.1, "pastry": 1.2, "cake": 1.1}
        }
        
        # Track daily sales for realistic patterns
        self.daily_sales_count = 0
        self.last_reset = datetime.now().date()
        
    def get_current_season(self):
        month = datetime.now().month
        if month in [12, 1, 2]:
            return "winter"
        elif month in [3, 4, 5]:
            return "spring"
        elif month in [6, 7, 8]:
            return "summer"
        else:
            return "fall"
    
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
    
    def calculate_demand_multiplier(self, store, product):
        """Calculate demand based on multiple factors"""
        hour = datetime.now().hour
        day_of_week = datetime.now().weekday()
        
        # Base multiplier
        multiplier = 1.0
        
        # Store traffic pattern
        if hour in store["peak_hours"]:
            multiplier *= 1.5
        
        # Store type multiplier
        multiplier *= store["traffic_multiplier"]
        
        # Weekend boost
        if day_of_week >= 5:  # Saturday or Sunday
            multiplier *= 1.3
        
        # Product popularity
        multiplier *= product["popularity"]
        
        # Seasonal adjustment
        season = self.get_current_season()
        category = product["category"].lower()
        if category in ["bread", "pastry", "cake"]:
            multiplier *= self.seasonal_patterns[season].get(category, 1.0)
        
        # Time of day preferences
        time_of_day = self.get_time_of_day()
        if time_of_day == "morning" and category in ["pastry", "bread"]:
            multiplier *= 1.4
        elif time_of_day == "afternoon" and category == "cake":
            multiplier *= 1.2
        
        return multiplier
    
    def generate_sale_event(self):
        """Generate realistic sale event with all required fields"""
        # Reset daily counter if new day
        if datetime.now().date() > self.last_reset:
            self.daily_sales_count = 0
            self.last_reset = datetime.now().date()
        
        # Select store and calculate traffic
        store = random.choice(self.stores)
        
        # Select products with weighted probability based on demand
        products_with_weights = []
        for product in self.products:
            weight = self.calculate_demand_multiplier(store, product)
            products_with_weights.append((product, weight))
        
        # Weighted random selection
        products, weights = zip(*products_with_weights)
        product = random.choices(products, weights=weights)[0]
        
        # Customer selection
        customer = random.choice(self.customers)
        
        # Quantity based on customer segment and product type
        if customer["segment"] == "business" and product["category"] in ["Pastry", "Muffin"]:
            quantity = random.randint(5, 12)  # Bulk orders
        elif product["category"] == "Cake":
            quantity = 1  # Usually single cakes
        else:
            quantity = random.randint(1, 4)
        
        # Apply customer spending pattern
        quantity = int(quantity * customer["spend_multiplier"])
        quantity = max(1, quantity)
        
        # Calculate pricing with potential discounts
        base_price = product["price"]
        discount_percentage = 0
        
        # Member discounts
        if customer["is_member"]:
            discount_percentage = random.choice([5, 10])
        
        # Time-based discounts (end of day for perishables)
        hour = datetime.now().hour
        if hour >= 19 and product["shelf_life_hours"] <= 24:
            discount_percentage = max(discount_percentage, 20)
        
        # Apply discount
        unit_price = base_price * (1 - discount_percentage / 100)
        total_price = unit_price * quantity
        
        # Generate event matching bronze schema
        event = {
            "event_id": f"pos_{store['id']}_{int(time.time()*1000)}",
            "event_time": datetime.now().isoformat(),
            "ingestion_time": datetime.now().isoformat(),
            "product_id": product["id"],
            "store_id": store["id"],
            "quantity": quantity,
            "unit_price": round(unit_price, 2),
            "customer_id": customer["id"],
            "Date": datetime.now().date().isoformat(),  # Added to match schema
            "processing_status": "PENDING",
            # Additional fields for enrichment
            "raw_payload": {
                "product_name": product["name"],
                "product_category": product["category"],
                "customer_segment": customer["segment"],
                "is_member": customer["is_member"],
                "payment_method": random.choice(["cash", "credit", "debit", "mobile"]),
                "time_of_day": self.get_time_of_day(),
                "discount_applied": discount_percentage,
                "total_price": round(total_price, 2),
                "store_type": store["type"],
                "cashier_id": f"cashier_{random.randint(1, 5)}",
                "pos_terminal": f"POS_{store['id']}_{random.randint(1, 3)}"
            }
        }
        
        self.daily_sales_count += 1
        
        return event
    
    def generate_error_event(self):
        """Occasionally generate events with data quality issues"""
        event = self.generate_sale_event()
        
        error_type = random.choice([
            "missing_customer",
            "negative_quantity",
            "zero_price",
            "future_timestamp"
        ])
        
        if error_type == "missing_customer":
            event["customer_id"] = None
        elif error_type == "negative_quantity":
            event["quantity"] = -1
        elif error_type == "zero_price":
            event["unit_price"] = 0
        elif error_type == "future_timestamp":
            event["event_time"] = (datetime.now() + timedelta(hours=1)).isoformat()
        
        event["processing_status"] = "ERROR"
        return event
    
    def run(self):
        print("Starting Enhanced POS Producer...")
        print(f"Generating sales for {len(self.stores)} stores with {len(self.customers)} customers")
        
        while True:
            try:
                # Occasionally generate error events (2% chance)
                if random.random() < 0.02:
                    event = self.generate_error_event()
                else:
                    event = self.generate_sale_event()
                
                # Send to Kafka
                self.producer.send(
                    'bakery-pos-events',
                    key={"store_id": event["store_id"]},
                    value=event
                )
                
                # Log significant events
                if event["processing_status"] == "ERROR":
                    print(f"⚠️  Error event: {event['event_id']}")
                elif event["quantity"] > 10:
                    print(f"📦 Bulk order: {event['event_id']} - Qty: {event['quantity']}")
                elif self.daily_sales_count % 100 == 0:
                    print(f"✅ Daily sales: {self.daily_sales_count}")
                
                # Variable rate based on store traffic patterns
                hour = datetime.now().hour
                store = next(s for s in self.stores if s["id"] == event["store_id"])
                
                if hour in store["peak_hours"]:
                    delay = random.uniform(0.5, 1.5)  # Peak hours - more frequent
                elif 6 <= hour <= 21:
                    delay = random.uniform(2, 4)  # Normal hours
                else:
                    delay = random.uniform(10, 20)  # Night hours - sparse
                
                time.sleep(delay)
                
            except Exception as e:
                print(f"Error producing message: {e}")
                time.sleep(5)

if __name__ == "__main__":
    producer = POSProducer()
    producer.run()