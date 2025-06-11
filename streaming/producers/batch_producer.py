import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import os
import requests

class BatchDataProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        
        self.products = list(range(1, 9))
        self.stores = list(range(1, 6))
        
        # Weather conditions and their impact on sales
        self.weather_conditions = {
            "Sunny": {"traffic_impact": 1.2, "hot_items": ["muffin", "cake"]},
            "Cloudy": {"traffic_impact": 1.0, "hot_items": ["bread", "pastry"]},
            "Rainy": {"traffic_impact": 0.7, "hot_items": ["bread", "cake"]},
            "Snowy": {"traffic_impact": 0.5, "hot_items": ["bread", "pastry"]},
            "Windy": {"traffic_impact": 0.9, "hot_items": ["bread"]},
            "Foggy": {"traffic_impact": 0.8, "hot_items": ["pastry", "muffin"]}
        }
        
        self.last_weather_update = {}
        self.active_promotions = []
    
    def generate_promotion_event(self):
        """Generate promotion events"""
        promo_types = ["DISCOUNT", "BOGO", "BUNDLE", "SEASONAL", "CLEARANCE"]
        
        # Determine promotion duration
        if random.random() < 0.7:
            # Short promotion (1-7 days)
            duration_days = random.randint(1, 7)
        else:
            # Long promotion (2-4 weeks)
            duration_days = random.randint(14, 28)
        
        start_date = datetime.now().date()
        end_date = (datetime.now() + timedelta(days=duration_days)).date()
        
        promo_type = random.choice(promo_types)
        
        # Set discount based on promotion type
        if promo_type == "CLEARANCE":
            discount_percentage = random.randint(30, 50)
        elif promo_type == "SEASONAL":
            discount_percentage = random.randint(15, 25)
        else:
            discount_percentage = random.randint(10, 20)
        
        event = {
            "promo_id": f"promo_{int(time.time()*1000)}",
            "product_id": random.choice(self.products),
            "promo_type": promo_type,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "discount_percentage": discount_percentage,
            "raw_payload": {
                "promo_name": f"{promo_type} Sale - {discount_percentage}% off",
                "target_audience": random.choice(["All Customers", "Members Only", "New Customers", "Students"]),
                "min_quantity": 1 if promo_type != "BUNDLE" else random.randint(2, 5),
                "max_quantity": None if promo_type != "CLEARANCE" else random.randint(5, 10),
                "terms_conditions": "Valid while supplies last. Cannot be combined with other offers.",
                "created_by": "marketing_team",
                "approved_by": "store_manager",
                "budget": random.randint(500, 5000)
            },
            "processing_status": "PENDING"
        }
        
        self.active_promotions.append({
            "promo_id": event["promo_id"],
            "end_date": end_date,
            "product_id": event["product_id"]
        })
        
        return event
    
    def generate_weather_event(self):
        """Generate weather data events"""
        # Simulate weather API call
        for store_id in self.stores:
            # Weather changes gradually
            if store_id in self.last_weather_update:
                last_condition = self.last_weather_update[store_id]
                # 70% chance weather stays the same
                if random.random() < 0.7:
                    condition = last_condition
                else:
                    condition = random.choice(list(self.weather_conditions.keys()))
            else:
                condition = random.choice(list(self.weather_conditions.keys()))
            
            self.last_weather_update[store_id] = condition
            
            # Generate weather metrics
            if condition == "Sunny":
                temp = random.randint(20, 30)
                humidity = random.randint(40, 60)
                wind_speed = random.randint(5, 15)
            elif condition == "Rainy":
                temp = random.randint(10, 20)
                humidity = random.randint(70, 95)
                wind_speed = random.randint(10, 25)
            elif condition == "Snowy":
                temp = random.randint(-5, 5)
                humidity = random.randint(60, 80)
                wind_speed = random.randint(15, 30)
            else:
                temp = random.randint(10, 25)
                humidity = random.randint(50, 70)
                wind_speed = random.randint(5, 20)
            
            event = {
                "weather_id": f"weather_{store_id}_{datetime.now().date().isoformat()}",
                "date": datetime.now().date().isoformat(),
                "store_id": store_id,
                "weather_condition": condition,
                "raw_payload": {
                    "temperature_celsius": temp,
                    "humidity": humidity,
                    "wind_speed": wind_speed,
                    "precipitation_mm": random.randint(0, 50) if condition == "Rainy" else 0,
                    "visibility_km": random.randint(1, 10),
                    "pressure_hpa": random.randint(990, 1030),
                    "uv_index": random.randint(1, 11) if condition == "Sunny" else random.randint(1, 3),
                    "forecast_accuracy": random.randint(85, 99),
                    "traffic_impact": self.weather_conditions[condition]["traffic_impact"],
                    "recommended_products": self.weather_conditions[condition]["hot_items"]
                },
                "processing_status": "PENDING"
            }
            
            yield event
    
    def cleanup_expired_promotions(self):
        """Remove expired promotions from active list"""
        current_date = datetime.now().date()
        self.active_promotions = [p for p in self.active_promotions if p["end_date"] >= current_date]
    
    def run(self):
        print("Starting Batch Data Producer (Promotions & Weather)...")
        
        while True:
            try:
                current_hour = datetime.now().hour
                
                # Weather updates every hour
                if datetime.now().minute < 5:  # First 5 minutes of each hour
                    print("☁️  Generating weather updates...")
                    for weather_event in self.generate_weather_event():
                        self.producer.send(
                            'bakery-weather-data',
                            value=weather_event
                        )
                    time.sleep(300)  # Wait 5 minutes
                
                # Promotions are created during business hours
                if 9 <= current_hour <= 17 and random.random() < 0.1:
                    promo_event = self.generate_promotion_event()
                    self.producer.send(
                        'bakery-promotions',
                        value=promo_event
                    )
                    print(f"🎯 New promotion: {promo_event['promo_type']} - "
                          f"{promo_event['discount_percentage']}% off Product {promo_event['product_id']}")
                
                # Cleanup expired promotions daily
                if current_hour == 0 and datetime.now().minute < 10:
                    old_count = len(self.active_promotions)
                    self.cleanup_expired_promotions()
                    if old_count > len(self.active_promotions):
                        print(f"🧹 Cleaned up {old_count - len(self.active_promotions)} expired promotions")
                
                # Wait before next iteration
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                print(f"Error in batch producer: {e}")
                time.sleep(5)

if __name__ == "__main__":
    producer = BatchDataProducer()
    producer.run()