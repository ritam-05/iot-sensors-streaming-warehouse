import os
import json
import boto3
import duckdb
from datetime import datetime, timezone
from dotenv import load_dotenv

# Force load variables from .env
load_dotenv()

def run_ingestion():
    print("Establishing connection to MotherDuck...")
    # This automatically uses the MOTHERDUCK_TOKEN environment variable
    con = duckdb.connect('md:iotstream_db')
    
    print("Ensuring database and bronze_telemetry table exist...")
    con.sql("""
        CREATE TABLE IF NOT EXISTS bronze_telemetry (
            event_id VARCHAR,
            device_id VARCHAR,
            machine_id VARCHAR,
            temperature DOUBLE,
            humidity DOUBLE,
            vibration DOUBLE,
            pressure DOUBLE,
            battery INTEGER,
            device_status VARCHAR,
            anomaly_status VARCHAR,
            anomaly_reasons VARCHAR,
            event_timestamp TIMESTAMP,
            processing_timestamp TIMESTAMP,
            schema_version INTEGER
        )
    """)

    warehouse_queue_url = os.environ.get("WAREHOUSE_QUEUE_URL")
    if not warehouse_queue_url:
        print("Error: WAREHOUSE_QUEUE_URL is not set in .env")
        return

    sqs_client = boto3.client('sqs')
    
    print("Polling Warehouse Queue for ready events...")
    response = sqs_client.receive_message(
        QueueUrl=warehouse_queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=5
    )
    
    messages = response.get('Messages', [])
    
    if not messages:
        print("No events found in the queue. Ingestion complete.")
        return
        
    print(f"Retrieved {len(messages)} events. Inserting into MotherDuck...")
    
    entries_to_delete = []
    
    for msg in messages:
        body = json.loads(msg['Body'])
        
        # Safely extract variables
        event_id = body.get('event_id')
        device_id = body.get('device_id')
        machine_id = body.get('machine_id')
        temperature = body.get('temperature')
        humidity = body.get('humidity')
        vibration = body.get('vibration')
        pressure = body.get('pressure')
        battery = body.get('battery')
        device_status = body.get('device_status')
        anomaly_status = body.get('anomaly_status', 'NORMAL')
        anomaly_reasons = json.dumps(body.get('anomaly_reasons', []))
        event_timestamp = body.get('event_timestamp')
        
        # The fix: Safely generate the processing timestamp if it isn't in the AWS payload
        processing_timestamp = body.get('processing_timestamp', datetime.now(timezone.utc).isoformat())
        schema_version = body.get('schema_version', 1)
        
        # Insert into MotherDuck
        con.execute("""
            INSERT INTO bronze_telemetry (
                event_id, device_id, machine_id, temperature, humidity, 
                vibration, pressure, battery, device_status, anomaly_status, 
                anomaly_reasons, event_timestamp, processing_timestamp, schema_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id, device_id, machine_id, temperature, humidity,
            vibration, pressure, battery, device_status, anomaly_status,
            anomaly_reasons, event_timestamp, processing_timestamp, schema_version
        ))
        
        # Mark for deletion so SQS doesn't send it again
        entries_to_delete.append({
            'Id': msg['MessageId'],
            'ReceiptHandle': msg['ReceiptHandle']
        })
        
    # Delete processed messages from the queue
    if entries_to_delete:
        sqs_client.delete_message_batch(
            QueueUrl=warehouse_queue_url,
            Entries=entries_to_delete
        )
        print(f"Batch ingestion complete. Deleted {len(entries_to_delete)} messages from SQS.")

if __name__ == "__main__":
    run_ingestion()