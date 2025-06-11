import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import os

fake = Faker()

class FeedbackProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        
        self.products = list(range(1, 9))
        self.platforms = ["Google", "Yelp", "TripAdvisor", "Facebook", "InStore", "Mobile App"]
        
        # Review templates by rating
        self.review_templates = {
            5: [
                "Amazing {product}! Best I've ever had.",
                "The {product} was absolutely delicious. Will definitely come back!",
                "Perfect {product}, fresh and tasty. Highly recommend!",
                "Love this bakery! The {product} is to die for."
            ],
            4: [
                "Really good {product}, just a bit pricey.",
                "The {product} was great, service could be faster though.",
                "Enjoyed the {product}, will try other items next time.",
                "Good quality {product}, consistent taste."
            ],
            3: [
                "The {product} was okay, nothing special.",
                "Average {product}, I've had better.",
                "Decent {product} but not worth the wait.",
                "The {product} was fine, just expected more."
            ],
            2: [
                "Disappointed with the {product}, was dry.",
                "The {product} didn't taste fresh.",
                "Not impressed with the {product}, won't order again.",
                "The {product} was stale, poor quality."
            ],
            1: [
                "Terrible {product}, completely inedible.",
                "Worst {product} I've ever had. Avoid!",
                "The {product} was old and tasted bad.",
                "Horrible experience with the {product}. Never again."
            ]
        }
        
        self.product_names = {
            1: "croissant", 2: "baguette", 3: "chocolate cake",
            4: "apple pie", 5: "sourdough bread", 6: "blueberry muffin",
            7: "cinnamon roll", 8: "cheesecake"
        }
    
    def generate_sentiment(self, rating):
        """Determine sentiment based on rating"""
        if rating >= 4:
            return "POSITIVE"
        elif rating == 3:
            return "NEUTRAL"
        else:
            return "NEGATIVE"
    
    def generate_feedback_event(self):
        """Generate customer feedback event"""
        # Rating distribution (skewed positive as typical for bakeries)
        rating_weights = [0.05, 0.10, 0.15, 0.30, 0.40]  # 1-5 stars
        rating = random.choices(range(1, 6), weights=rating_weights)[0]
        
        product_id = random.choice(self.products)
        product_name = self.product_names[product_id]
        
        # Generate review text
        review_template = random.choice(self.review_templates[rating])
        review_text = review_template.format(product=product_name)
        
        # Add additional comments sometimes
        if random.random() < 0.3:
            if rating >= 4:
                review_text += " " + random.choice([
                    "Great customer service too!",
                    "The staff was very friendly.",
                    "Clean and welcoming atmosphere.",
                    "Reasonable prices for the quality."
                ])
            else:
                review_text += " " + random.choice([
                    "Staff seemed overwhelmed.",
                    "Long wait times.",
                    "Place could use some cleaning.",
                    "Overpriced for what you get."
                ])
        
        # Feedback can be delayed (reviews posted later)
        if random.random() < 0.4:
            # Delayed feedback
            delay_hours = random.randint(1, 72)
            feedback_time = datetime.now() - timedelta(hours=delay_hours)
        else:
            feedback_time = datetime.now()
        
        event = {
            "feedback_id": f"fb_{int(time.time()*1000)}",
            "feedback_time": feedback_time.isoformat(),
            "ingestion_time": datetime.now().isoformat(),
            "customer_id": fake.uuid4(),
            "product_id": product_id,
            "rating": rating,
            "platform": random.choice(self.platforms),
            "review_text": review_text[:100],  # Limit to 100 chars as per schema
            "raw_payload": {
                "full_review": review_text,
                "sentiment": self.generate_sentiment(rating),
                "verified_purchase": random.choice([True, False]),
                "helpful_count": random.randint(0, 50) if rating in [1, 5] else random.randint(0, 10),
                "response_from_owner": "Thank you for your feedback!" if rating <= 2 and random.random() < 0.3 else None,
                "language": "en",
                "location": random.choice(["Downtown", "Mall", "Airport", "Suburban", "University"])
            },
            "processing_status": "PENDING"
        }
        
        return event
    
    def run(self):
        print("Starting Customer Feedback Producer...")
        
        while True:
            try:
                # Feedback comes in spurts
                feedback_burst = random.randint(1, 5)
                
                for _ in range(feedback_burst):
                    event = self.generate_feedback_event()
                    
                    self.producer.send(
                        'bakery-customer-feedback',
                        value=event
                    )
                    
                    sentiment = event["raw_payload"]["sentiment"]
                    if sentiment == "NEGATIVE":
                        print(f"👎 Negative feedback: {event['rating']}⭐ for Product {event['product_id']}")
                    elif sentiment == "POSITIVE" and event["rating"] == 5:
                        print(f"🌟 5-star review for Product {event['product_id']}!")
                
                # Wait between bursts (reviews don't come continuously)
                time.sleep(random.uniform(60, 300))  # 1-5 minutes
                
            except Exception as e:
                print(f"Error producing feedback: {e}")
                time.sleep(5)

if __name__ == "__main__":
    producer = FeedbackProducer()
    producer.run()