import os
import json
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError

class DynamoDBStateStore:
    def __init__(self):
        # Using boto3.resource instead of client for simpler dictionary insertion
        self.dynamodb = boto3.resource('dynamodb')
        self.table_name = os.environ.get("DYNAMODB_TABLE_NAME", "iotstream_device_state")
        self.table = self.dynamodb.Table(self.table_name)

    def update_state(self, processed_event: dict) -> bool:
        """
        Upserts the latest device state into DynamoDB.
        Automatically converts Python floats to DynamoDB-compatible Decimals.
        """
        try:
            # Convert standard floats to Decimals safely using JSON parsing
            dynamo_item = json.loads(json.dumps(processed_event), parse_float=Decimal)
            
            self.table.put_item(Item=dynamo_item)
            return True
        except ClientError as e:
            print(f"Failed to update DynamoDB for {processed_event.get('device_id')}: {e}")
            return False

# Local test block
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    # Sample transformed event containing floats
    test_event = {
        "device_id": "sensor-001",
        "machine_id": "MACHINE-01",
        "event_timestamp": "2026-11-03T12:00:00Z",
        "processing_timestamp": "2026-11-03T13:10:00Z",
        "temperature": 92.5,
        "humidity": 40.0,
        "vibration": 3.0,
        "pressure": 100.0,
        "battery": 80,
        "device_status": "ONLINE",
        "anomaly_status": "CRITICAL",
        "anomaly_reasons": ["CRITICAL_TEMPERATURE"],
        "schema_version": 1,
        "processing_version": "1.0"
    }
    
    store = DynamoDBStateStore()
    print(f"Writing real-time state for {test_event['device_id']} to DynamoDB...")
    
    if store.update_state(test_event):
        print("✅ Successfully wrote to DynamoDB!")
    else:
        print("❌ Failed to write to DynamoDB.")