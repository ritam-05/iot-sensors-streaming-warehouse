import os
import json
import uuid
import random
from datetime import datetime, timezone
import boto3

# Initialize outside handler for warm-start efficiency
sqs_client = boto3.client('sqs')

def lambda_handler(event, context):
    """Triggered automatically by EventBridge every minute."""
    queue_url = os.environ.get("TELEMETRY_QUEUE_URL")
    
    # Simulate a burst of 5 to 15 concurrent sensor readings
    num_events = random.randint(5, 15)
    entries = []
    
    for _ in range(num_events):
        machine_id = f"MACHINE-{random.randint(1, 10):02d}"
        
        payload = {
            "event_id": f"evt-{uuid.uuid4().hex[:8]}",
            "device_id": f"sensor-{machine_id[-2:]}",
            "machine_id": machine_id,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature": round(random.uniform(60.0, 95.0), 2),
            "humidity": round(random.uniform(30.0, 60.0), 2),
            "vibration": round(random.uniform(1.0, 6.0), 2),
            "pressure": round(random.uniform(90.0, 110.0), 2),
            "battery": random.randint(5, 100),
            "device_status": random.choice(["ONLINE", "ONLINE", "ONLINE", "OFFLINE"]),
            "schema_version": 1
        }
        
        entries.append({
            'Id': payload['event_id'],
            'MessageBody': json.dumps(payload)
        })
        
    if entries and queue_url:
        # AWS SQS only accepts 10 messages per batch request. Chunk the list.
        for i in range(0, len(entries), 10):
            batch = entries[i:i+10]
            sqs_client.send_message_batch(QueueUrl=queue_url, Entries=batch)
            
    print(f"Autonomous cycle complete: Pushed {len(entries)} events to SQS.")
    return {"statusCode": 200, "body": f"Generated {len(entries)} events."}

# Keep local testing ability
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    lambda_handler({}, None)