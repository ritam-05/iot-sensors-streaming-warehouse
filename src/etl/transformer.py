from datetime import datetime, timezone
from src.etl.anomaly_detector import AnomalyDetector

class ETLTransformer:
    def __init__(self):
        self.detector = AnomalyDetector()
        self.processing_version = "1.0"

    def transform(self, raw_event: dict, receipt_handle: str = "local-test") -> dict:
        """Cleans, enriches, and classifies a raw telemetry event."""
        
        # 1. Metadata Enrichment
        current_time = datetime.now(timezone.utc).isoformat()
        
        # 2. Anomaly Detection
        anomaly_metadata = self.detector.evaluate(raw_event)
        
        # 3. Construct the Processed Payload
        processed_event = {
            "event_id": raw_event["event_id"],
            "device_id": raw_event["device_id"],
            "machine_id": raw_event["machine_id"],
            "event_timestamp": raw_event["event_timestamp"],
            "processing_timestamp": current_time,
            "temperature": float(raw_event["temperature"]),
            "humidity": float(raw_event["humidity"]),
            "vibration": float(raw_event["vibration"]),
            "pressure": float(raw_event["pressure"]),
            "battery": int(raw_event["battery"]),
            "device_status": str(raw_event["device_status"]).upper(),
            "anomaly_status": anomaly_metadata["anomaly_status"],
            "anomaly_reasons": anomaly_metadata["anomaly_reasons"],
            "schema_version": raw_event.get("schema_version", 1),
            "processing_version": self.processing_version
        }
        
        return processed_event

# Local test block
if __name__ == "__main__":
    import json
    
    # Simulating a high-temperature raw event
    sample_event = {
        "event_id": "evt-test123",
        "device_id": "sensor-001",
        "machine_id": "MACHINE-01",
        "event_timestamp": "2026-11-03T12:00:00Z",
        "temperature": 92.5,
        "humidity": 40.0,
        "vibration": 3.0,
        "pressure": 100.0,
        "battery": 80,
        "device_status": "ONLINE"
    }
    
    transformer = ETLTransformer()
    result = transformer.transform(sample_event)
    print("Transformation Successful! Processed Event:")
    print(json.dumps(result, indent=2))