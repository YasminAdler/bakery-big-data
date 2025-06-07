import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import os

class IoTProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Equipment profiles
        self.equipment = [
            {"id": 1, "type": "oven", "name": "Main Oven 1", "base_temp": 220},
            {"id": 2, "type": "oven", "name": "Main Oven 2", "base_temp": 210},
            {"id": 3, "type": "mixer", "name": "Dough Mixer 1", "base_temp": 25},
            {"id": 4, "type": "mixer", "name": "Dough Mixer 2", "base_temp": 25},
            {"id": 5, "type": "proofer", "name": "Proofer 1", "base_temp": 35},
            {"id": 6, "type": "freezer", "name": "Storage Freezer", "base_temp": -18}
        ]
        
        # Equipment state tracking
        self.equipment_state = {
            eq["id"]: {
                "status": "RUNNING",
                "runtime": 0,
                "last_maintenance": 0
            } for eq in self.equipment
        }
    
    def generate_sensor_reading(self, equipment):
        """Generate realistic sensor readings based on equipment type"""
        eq_id = equipment["id"]
        eq_type = equipment["type"]
        state = self.equipment_state[eq_id]
        
        # Temperature variations based on equipment type and status
        if state["status"] == "RUNNING":
            if eq_type == "oven":
                temp = equipment["base_temp"] + random.gauss(0, 10)
                power = 80 + random.gauss(0, 10)
            elif eq_type == "mixer":
                temp = equipment["base_temp"] + random.gauss(0, 2)
                power = 20 + random.gauss(0, 5)
            elif eq_type == "proofer":
                temp = equipment["base_temp"] + random.gauss(0, 1)
                power = 15 + random.gauss(0, 3)
            else:  # freezer
                temp = equipment["base_temp"] + random.gauss(0, 0.5)
                power = 30 + random.gauss(0, 5)
        else:
            temp = 20 + random.gauss(0, 2)  # Room temperature
            power = 1 + random.gauss(0, 0.5)  # Standby power
        
        # Simulate equipment issues
        error_code = None
        if state["runtime"] > 1000 and random.random() < 0.01:
            error_code = random.choice(["E001", "E002", "W001"])
            state["status"] = "ERROR" if error_code.startswith("E") else "WARNING"
        
        # Generate event
        event = {
            "metric_id": f"iot_{eq_id}_{int(time.time()*1000)}",
            "equipment_id": eq_id,
            "timestamp": datetime.now().isoformat(),
            "temperature": round(temp, 2),
            "humidity": round(45 + random.gauss(0, 5), 2),
            "power_consumption": round(max(0, power), 2),
            "vibration_level": round(random.uniform(0, 5), 2),
            "operational_status": state["status"],
            "error_code": error_code
        }
        
        # Update state
        if state["status"] == "RUNNING":
            state["runtime"] += 1
        
        return event
    
    def simulate_equipment_lifecycle(self):
        """Simulate equipment turning on/off based on bakery operations"""
        current_hour = datetime.now().hour
        
        for eq_id, state in self.equipment_state.items():
            equipment = next(e for e in self.equipment if e["id"] == eq_id)
            
            # Ovens run during baking hours (4 AM - 8 PM)
            if equipment["type"] == "oven":
                if 4 <= current_hour <= 20:
                    if state["status"] == "OFF" and random.random() < 0.1:
                        state["status"] = "RUNNING"
                else:
                    if state["status"] == "RUNNING" and random.random() < 0.3:
                        state["status"] = "OFF"
            
            # Mixers run in batches
            elif equipment["type"] == "mixer":
                if state["status"] == "OFF" and random.random() < 0.05:
                    state["status"] = "RUNNING"
                elif state["status"] == "RUNNING" and state["runtime"] % 30 == 0:
                    state["status"] = "OFF"
                    state["runtime"] = 0
            
            # Freezer always on
            elif equipment["type"] == "freezer":
                state["status"] = "RUNNING"
            
            # Clear errors occasionally
            if state["status"] == "ERROR" and random.random() < 0.1:
                state["status"] = "RUNNING"
                state["runtime"] = 0
    
    def run(self):
        print("Starting IoT Producer...")
        
        while True:
            try:
                # Update equipment states
                self.simulate_equipment_lifecycle()
                
                # Generate readings for all equipment
                for equipment in self.equipment:
                    event = self.generate_sensor_reading(equipment)
                    
                    self.producer.send(
                        'bakery-iot-events',
                        value=event
                    )
                    
                    if event["operational_status"] != "RUNNING":
                        print(f"Equipment {equipment['name']} - "
                              f"Status: {event['operational_status']}, "
                              f"Temp: {event['temperature']}°C")
                
                # Send readings every 10 seconds
                time.sleep(10)
                
            except Exception as e:
                print(f"Error producing IoT message: {e}")
                time.sleep(5)

if __name__ == "__main__":
    producer = IoTProducer()
    producer.run()