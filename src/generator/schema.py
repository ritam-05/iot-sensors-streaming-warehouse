from pydantic import BaseModel, Field, field_validator
from typing import Literal
from datetime import datetime
import json

class TelemetryEvent(BaseModel):
    event_id: str = Field(..., description="Unique identifier for the event")
    device_id: str = Field(..., description="ID of the sensor device")
    machine_id: str = Field(..., description="ID of the parent machine")
    event_timestamp: datetime = Field(..., description="UTC timestamp of the reading")
    temperature: float = Field(..., description="Temperature in Celsius")
    humidity: float = Field(..., description="Humidity percentage")
    vibration: float = Field(..., description="Vibration frequency")
    pressure: float = Field(..., description="Pressure reading")
    battery: int = Field(..., ge=0, le=100, description="Battery percentage (0-100)")
    device_status: Literal["ONLINE", "OFFLINE", "ERROR"] = Field(..., description="Operational status")
    schema_version: int = Field(default=1, description="Schema version control")

    @field_validator("event_timestamp")
    def ensure_utc(cls, v):
        if v.tzinfo is None:
            raise ValueError("Timestamp must include timezone information (UTC)")
        return v

if __name__ == "__main__":
    print("Testing TelemetryEvent Schema...")
    
    # Simulating a valid raw payload from Phase 2
    raw_payload = {
        "event_id": "evt-000001",
        "device_id": "sensor-001",
        "machine_id": "machine-01",
        "event_timestamp": "2026-11-02T10:30:00Z",
        "temperature": 76.5,
        "humidity": 48.2,
        "vibration": 3.4,
        "pressure": 101.3,
        "battery": 92,
        "device_status": "ONLINE",
        "schema_version": 1
    }
    
    try:
        validated_event = TelemetryEvent(**raw_payload)
        print("✅ Validation Successful!")
        print(validated_event.model_dump_json(indent=2))
    except Exception as e:
        print(f"❌ Validation Failed:\n{e}")
