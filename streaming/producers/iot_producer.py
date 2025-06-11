import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import os
import numpy as np

class IoTProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        
        # Enhanced equipment profiles with more realistic parameters
        self.equipment = [
            {
                "id": 1, 
                "type": "oven", 
                "name": "Main Oven 1", 
                "base_temp": 220,
                "temp_variance": 10,
                "power_rating": 15.5,  # kW from schema
                "maintenance_frequency": "Monthly",
                "age_days": 730,  # 2 years old
                "efficiency": 0.95
            },
            {
                "id": 2, 
                "type": "oven", 
                "name": "Main Oven 2", 
                "base_temp": 210,
                "temp_variance": 12,
                "power_rating": 15.5,
                "maintenance_frequency": "Monthly",
                "age_days": 365,  # 1 year old
                "efficiency": 0.98
            },
            {
                "id": 3, 
                "type": "mixer", 
                "name": "Dough Mixer 1", 
                "base_temp": 25,
                "temp_variance": 2,
                "power_rating": 5.0,
                "maintenance_frequency": "Weekly",
                "age_days": 1095,  # 3 years old
                "efficiency": 0.90
            },
            {
                "id": 4, 
                "type": "mixer", 
                "name": "Dough Mixer 2", 
                "base_temp": 25,
                "temp_variance": 2,
                "power_rating": 5.0,
                "maintenance_frequency": "Weekly", 
                "age_days": 180,  # 6 months old
                "efficiency": 0.99
            },
            {
                "id": 5, 
                "type": "proofer", 
                "name": "Proofer 1", 
                "base_temp": 35,
                "temp_variance": 1,
                "power_rating": 3.0,
                "maintenance_frequency": "Monthly",
                "age_days": 545,  # 1.5 years old
                "efficiency": 0.96
            },
            {
                "id": 6, 
                "type": "freezer", 
                "name": "Storage Freezer", 
                "base_temp": -18,
                "temp_variance": 0.5,
                "power_rating": 8.0,
                "maintenance_frequency": "Quarterly",
                "age_days": 2190,  # 6 years old
                "efficiency": 0.85
            }
        ]
        
        # Enhanced equipment state tracking
        self.equipment_state = {}
        for eq in self.equipment:
            self.equipment_state[eq["id"]] = {
                "status": "RUNNING",
                "runtime_minutes": 0,
                "total_runtime_hours": random.randint(1000, 5000),
                "last_maintenance_date": datetime.now() - timedelta(days=random.randint(1, 30)),
                "cycles_since_maintenance": 0,
                "temperature_history": [],
                "power_history": [],
                "error_count": 0,
                "efficiency_degradation": 0
            }
    
    def calculate_equipment_stress(self, equipment, state):
        """Calculate stress factors affecting equipment performance"""
        stress = 0
        
        # Age stress
        age_factor = equipment["age_days"] / 365  # Years
        stress += age_factor * 0.1
        
        # Runtime stress
        if state["runtime_minutes"] > 480:  # 8 hours continuous
            stress += 0.2
        
        # Maintenance overdue stress
        days_since_maintenance = (datetime.now() - state["last_maintenance_date"]).days
        if equipment["maintenance_frequency"] == "Weekly" and days_since_maintenance > 7:
            stress += 0.3
        elif equipment["maintenance_frequency"] == "Monthly" and days_since_maintenance > 30:
            stress += 0.3
        elif equipment["maintenance_frequency"] == "Quarterly" and days_since_maintenance > 90:
            stress += 0.3
        
        # Error accumulation stress
        if state["error_count"] > 5:
            stress += 0.2
        
        return min(stress, 1.0)  # Cap at 1.0
    
    def generate_sensor_reading(self, equipment):
        """Generate realistic sensor readings with degradation over time"""
        eq_id = equipment["id"]
        eq_type = equipment["type"]
        state = self.equipment_state[eq_id]
        
        # Calculate stress factor
        stress = self.calculate_equipment_stress(equipment, state)
        
        # Temperature calculations with stress impact
        if state["status"] == "RUNNING":
            # Base temperature with variance
            temp_variance = equipment["temp_variance"] * (1 + stress)
            temp = equipment["base_temp"] + random.gauss(0, temp_variance)
            
            # Power consumption with efficiency degradation
            base_power = equipment["power_rating"]
            efficiency = equipment["efficiency"] - state["efficiency_degradation"]
            power = base_power / efficiency
            
            # Add power variance based on load
            if eq_type == "oven":
                # Ovens cycle heating elements
                power *= (0.7 + 0.3 * abs(np.sin(time.time() / 300)))
            elif eq_type == "mixer":
                # Mixers vary based on dough resistance
                power *= random.uniform(0.8, 1.2)
            
            power += random.gauss(0, base_power * 0.1)
            
        elif state["status"] == "IDLE":
            temp = 20 + random.gauss(0, 2)  # Room temperature
            power = equipment["power_rating"] * 0.05  # Standby power
        else:  # ERROR or OFF
            temp = 20 + random.gauss(0, 2)
            power = 0
        
        # Additional sensor readings
        humidity = 45 + random.gauss(0, 5)
        vibration = random.uniform(0, 5) * (1 + stress)
        
        # Simulate equipment issues based on stress
        error_code = None
        if state["status"] == "RUNNING":
            # Increase error probability with stress
            if random.random() < (0.01 + stress * 0.05):
                if eq_type == "oven":
                    error_code = random.choice(["E001_OVERHEAT", "E002_ELEMENT_FAIL", "W001_DOOR_SEAL"])
                elif eq_type == "mixer":
                    error_code = random.choice(["E003_MOTOR_STRAIN", "W002_BEARING_WEAR"])
                elif eq_type == "freezer":
                    error_code = random.choice(["E004_COMPRESSOR", "W003_DEFROST_CYCLE"])
                else:
                    error_code = "W999_GENERAL"
                
                if error_code.startswith("E"):
                    state["status"] = "ERROR"
                    state["error_count"] += 1
                else:
                    state["status"] = "WARNING"
        
        # Track history for trend analysis
        state["temperature_history"].append(temp)
        state["power_history"].append(power)
        
        # Keep only last 100 readings
        if len(state["temperature_history"]) > 100:
            state["temperature_history"] = state["temperature_history"][-100:]
            state["power_history"] = state["power_history"][-100:]
        
        # Generate event matching bronze schema
        event = {
            "metric_id": f"iot_{eq_id}_{int(time.time()*1000)}",
            "equipment_id": eq_id,
            "event_time": datetime.now().isoformat(),
            "ingestion_time": datetime.now().isoformat(),
            "power_consumption": round(max(0, power), 2),
            "operational_status": state["status"],
            "raw_payload": {
                "equipment_name": equipment["name"],
                "equipment_type": equipment["type"],
                "temperature": round(temp, 2),
                "humidity": round(humidity, 2),
                "vibration_level": round(vibration, 2),
                "error_code": error_code,
                "runtime_minutes": state["runtime_minutes"],
                "total_runtime_hours": state["total_runtime_hours"],
                "efficiency": round(equipment["efficiency"] - state["efficiency_degradation"], 3),
                "stress_level": round(stress, 2),
                "cycles_since_maintenance": state["cycles_since_maintenance"],
                "sensor_diagnostics": {
                    "temp_sensor": "OK" if random.random() > 0.01 else "DEGRADED",
                    "power_sensor": "OK" if random.random() > 0.02 else "DEGRADED",
                    "vibration_sensor": "OK" if random.random() > 0.03 else "DEGRADED"
                }
            },
            "processing_status": "PENDING"
        }
        
        # Update state
        if state["status"] == "RUNNING":
            state["runtime_minutes"] += 0.167  # 10 seconds = 0.167 minutes
            state["total_runtime_hours"] += 0.00278  # 10 seconds in hours
            state["cycles_since_maintenance"] += 1
            
            # Gradual efficiency degradation
            state["efficiency_degradation"] += 0.00001
        
        return event
    
    def simulate_equipment_lifecycle(self):
        """Enhanced equipment lifecycle simulation"""
        current_hour = datetime.now().hour
        day_of_week = datetime.now().weekday()
        
        for eq_id, state in self.equipment_state.items():
            equipment = next(e for e in self.equipment if e["id"] == eq_id)
            
            # Ovens - follow baking schedule
            if equipment["type"] == "oven":
                if 4 <= current_hour <= 20:  # Baking hours
                    if state["status"] in ["OFF", "IDLE"] and random.random() < 0.2:
                        state["status"] = "RUNNING"
                        print(f"🔥 {equipment['name']} started")
                else:
                    if state["status"] == "RUNNING" and random.random() < 0.4:
                        state["status"] = "IDLE"
                        state["runtime_minutes"] = 0
            
            # Mixers - batch operation
            elif equipment["type"] == "mixer":
                if state["status"] == "IDLE" and 5 <= current_hour <= 18:
                    if random.random() < 0.1:  # Start new batch
                        state["status"] = "RUNNING"
                        print(f"🥖 {equipment['name']} started batch")
                elif state["status"] == "RUNNING":
                    # Typical mixing cycle is 20-30 minutes
                    if state["runtime_minutes"] > random.randint(20, 30):
                        state["status"] = "IDLE"
                        state["runtime_minutes"] = 0
                        print(f"✅ {equipment['name']} batch complete")
            
            # Proofer - follows mixer schedule
            elif equipment["type"] == "proofer":
                if 6 <= current_hour <= 19:
                    if state["status"] == "IDLE" and random.random() < 0.15:
                        state["status"] = "RUNNING"
                else:
                    if state["status"] == "RUNNING" and random.random() < 0.3:
                        state["status"] = "IDLE"
            
            # Freezer - always on, but may have defrost cycles
            elif equipment["type"] == "freezer":
                if state["status"] == "RUNNING":
                    # Defrost cycle every 12 hours
                    if state["total_runtime_hours"] % 12 < 0.1:
                        state["status"] = "IDLE"  # Defrost mode
                        print(f"❄️  {equipment['name']} defrost cycle")
                else:
                    state["status"] = "RUNNING"
            
            # Maintenance simulation
            days_since_maintenance = (datetime.now() - state["last_maintenance_date"]).days
            maintenance_due = False
            
            if equipment["maintenance_frequency"] == "Weekly" and days_since_maintenance >= 7:
                maintenance_due = True
            elif equipment["maintenance_frequency"] == "Monthly" and days_since_maintenance >= 30:
                maintenance_due = True
            elif equipment["maintenance_frequency"] == "Quarterly" and days_since_maintenance >= 90:
                maintenance_due = True
            
            if maintenance_due and day_of_week == 0 and current_hour == 6:  # Monday morning maintenance
                state["status"] = "MAINTENANCE"
                state["last_maintenance_date"] = datetime.now()
                state["cycles_since_maintenance"] = 0
                state["error_count"] = 0
                state["efficiency_degradation"] *= 0.5  # Maintenance improves efficiency
                print(f"🔧 {equipment['name']} under maintenance")
            
            # Clear errors after some time
            if state["status"] == "ERROR" and random.random() < 0.05:
                state["status"] = "IDLE"
                print(f"✅ {equipment['name']} error cleared")
            
            # End maintenance
            if state["status"] == "MAINTENANCE" and random.random() < 0.3:
                state["status"] = "IDLE"
                print(f"✅ {equipment['name']} maintenance complete")
    
    def generate_late_arrival_event(self, equipment):
        """Simulate late-arriving IoT data"""
        event = self.generate_sensor_reading(equipment)
        
        # Make the event appear to be from the past
        hours_late = random.randint(1, 48)
        event["event_time"] = (datetime.now() - timedelta(hours=hours_late)).isoformat()
        event["raw_payload"]["late_arrival"] = True
        event["raw_payload"]["delay_hours"] = hours_late
        event["processing_status"] = "LATE_ARRIVAL"
        
        return event
    
    def run(self):
        print("Starting Enhanced IoT Producer...")
        print(f"Monitoring {len(self.equipment)} equipment units")
        
        reading_counter = 0
        
        while True:
            try:
                # Update equipment states
                self.simulate_equipment_lifecycle()
                
                # Generate readings for all equipment
                for equipment in self.equipment:
                    # Occasionally simulate late arrivals (1% chance)
                    if random.random() < 0.01:
                        event = self.generate_late_arrival_event(equipment)
                        print(f"⏰ Late arrival: Equipment {equipment['id']} - {event['raw_payload']['delay_hours']}h late")
                    else:
                        event = self.generate_sensor_reading(equipment)
                    
                    self.producer.send(
                        'bakery-iot-events',
                        value=event
                    )
                    
                    # Log significant events
                    state = self.equipment_state[equipment["id"]]
                    if event["operational_status"] == "ERROR":
                        print(f"🚨 ERROR: {equipment['name']} - {event['raw_payload']['error_code']}")
                    elif event["operational_status"] == "WARNING":
                        print(f"⚠️  WARNING: {equipment['name']} - {event['raw_payload']['error_code']}")
                    elif state["stress_level"] > 0.7:
                        print(f"😰 High stress: {equipment['name']} - Stress: {state['stress_level']:.2f}")
                
                reading_counter += len(self.equipment)
                if reading_counter % 100 == 0:
                    print(f"📊 Total readings sent: {reading_counter}")
                
                # Send readings every 10 seconds as specified
                time.sleep(10)
                
            except Exception as e:
                print(f"Error producing IoT message: {e}")
                time.sleep(5)

if __name__ == "__main__":
    producer = IoTProducer()
    producer.run()