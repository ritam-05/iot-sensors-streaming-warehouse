import os
import json
import boto3
from decimal import Decimal
from anomaly_detector import AnomalyDetector

# Initialize outside the handler for warm-start performance
sqs_client = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
detector = AnomalyDetector()

def lambda_handler(event, context):
    warehouse_queue_url = os.environ.get("WAREHOUSE_QUEUE_URL")
    table_name = os.environ.get("DYNAMODB_TABLE_NAME")
    
    if not table_name:
        print("WARNING: DYNAMODB_TABLE_NAME environment variable is missing!")
        table = None
    else:
        table = dynamodb.Table(table_name)
    
    warehouse_entries = []
    
    # Process the batch of messages from the Telemetry Queue
    for record in event.get('Records', []):
        payload = json.loads(record['body'], parse_float=Decimal)
        
        # Enrich the payload with our anomaly detection logic
        anomaly_results = detector.evaluate(payload)
        payload.update(anomaly_results)
        
        # 1. Update the live state in DynamoDB
        if table:
            try:
                # Upsert the item into DynamoDB
                table.put_item(Item=payload)
            except Exception as e:
                print(f"DynamoDB Error: {e}")
        else:
            print("Skipping DynamoDB insert: Table name not configured.")
            
        # 2. Prepare the enriched event for the Warehouse Queue
        warehouse_entries.append({
            'Id': record['messageId'],
            'MessageBody': json.dumps(payload)
        })
        
    # Send the enriched batch to the Warehouse Queue for MotherDuck
    if warehouse_entries and warehouse_queue_url:
        # AWS SQS only accepts 10 messages per batch request. Chunk the list.
        for i in range(0, len(warehouse_entries), 10):
            batch = warehouse_entries[i:i+10]
            try:
                sqs_client.send_message_batch(QueueUrl=warehouse_queue_url, Entries=batch)
            except Exception as e:
                print(f"SQS Error: {e}")
            
    print(f"ETL cycle complete: Processed {len(warehouse_entries)} events.")
    return {"statusCode": 200, "body": f"Processed {len(warehouse_entries)} events."}

# Keep local testing ability
if __name__ == "__main__":
    # Mock event to allow running `python src/etl/handler.py` locally
    mock_event = {
        "Records": [
            {
                "messageId": "local-test-1",
                "body": json.dumps({
                    "event_id": "evt-local-1",
                    "device_id": "sensor-01",
                    "machine_id": "MACHINE-01",
                    "temperature": 95.0,
                    "humidity": 45.0,
                    "vibration": 5.5,
                    "battery": 15,
                    "device_status": "ONLINE"
                })
            }
        ]
    }
    lambda_handler(mock_event, None)
