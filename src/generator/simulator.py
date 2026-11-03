import random
import yaml
import uuid
import json
import os
from datetime import datetime, timezone

class IoTSimulator:
    def __init__(self, config_path="config/thresholds.yaml"):
        # Ensure we can find the config file regardless of where the script is run from
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        full_config_path = os.path.join(base_dir, config_path)
        
        with open(full_config_path, 'r') as f:
            self.thresholds = yaml.safe_load(f)
        
        # Initialize the 10 virtual devices
        self.devices = [
            {
                "device_id": f"sensor-{str(i).zfill(3)}", 
                "machine_id": f"MACHINE-{str(i).zfill(2)}"
            } 
            for i in range(1, 11)
        ]

    def generate_normal_reading(self) -> dict:
        """Generates realistic normal baseline telemetry."""
        return {
            "temperature": round(random.uniform(60.0, 74.0), 1),
            "humidity": round(random.uniform(30.0, 60.0), 1),
            "vibration": round(random.uniform(1.0, 6.0), 1),
            "pressure": round(random.uniform(90.0, 105.0), 1),
            "battery": random.randint(21, 100),
            "device_status": "ONLINE"
        }

    def inject_anomaly(self, reading: dict) -> dict:
        """Injects a controlled anomaly based on a 10% probability."""
        if random.random() < 0.10:
            anomaly_type = random.choice(["overheating", "high_vibration", "low_battery", "offline"])
            
            if anomaly_type == "overheating":
                reading["temperature"] = round(random.uniform(self.thresholds['temperature']['critical'], 100.0), 1)
            elif anomaly_type == "high_vibration":
                reading["vibration"] = round(random.uniform(self.thresholds['vibration']['critical'], 15.0), 1)
            elif anomaly_type == "low_battery":
                reading["battery"] = random.randint(1, self.thresholds['battery']['critical'])
            elif anomaly_type == "offline":
                reading["device_status"] = "OFFLINE"
                
        return reading

    def generate_batch(self) -> list:
        """Generates exactly one event per device."""
        events = []
        for device in self.devices:
            base_reading = self.generate_normal_reading()
            final_reading = self.inject_anomaly(base_reading)
            
            event = {
                "event_id": f"evt-{uuid.uuid4().hex[:8]}",
                "device_id": device["device_id"],
                "machine_id": device["machine_id"],
                "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                **final_reading
            }
            events.append(event)
        return events

# Local testing block
if __name__ == "__main__":
    print("Initializing IoT Simulator...")
    sim = IoTSimulator()
    batch = sim.generate_batch()
    print(f"Generated {len(batch)} events successfully. Sample output:\n")
    print(json.dumps(batch, indent=2))