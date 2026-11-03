import os
import yaml

class AnomalyDetector:
    def __init__(self, config_path="config/thresholds.yaml"):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        full_config_path = os.path.join(base_dir, config_path)
        
        with open(full_config_path, 'r') as f:
            self.thresholds = yaml.safe_load(f)

    def evaluate(self, event: dict) -> dict:
        """Evaluates sensor readings and returns anomaly metadata."""
        anomalies = []
        status = "NORMAL"

        if event.get("temperature", 0) >= self.thresholds["temperature"]["critical"]:
            anomalies.append("CRITICAL_TEMPERATURE")
        elif event.get("temperature", 0) >= self.thresholds["temperature"]["warning"]:
            anomalies.append("WARNING_TEMPERATURE")

        if event.get("vibration", 0) >= self.thresholds["vibration"]["critical"]:
            anomalies.append("CRITICAL_VIBRATION")

        if event.get("battery", 100) <= self.thresholds["battery"]["critical"]:
            anomalies.append("CRITICAL_BATTERY")

        if event.get("device_status") == "OFFLINE":
            anomalies.append("DEVICE_OFFLINE")

        if any("CRITICAL" in a or "OFFLINE" in a for a in anomalies):
            status = "CRITICAL"
        elif anomalies:
            status = "WARNING"

        return {
            "anomaly_status": status,
            "anomaly_reasons": anomalies
        }