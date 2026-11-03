import os
import json
import boto3
import duckdb
from dotenv import load_dotenv

load_dotenv()

def run_ingestion():
    queue_url = os.environ.get("WAREHOUSE_QUEUE_URL")
    sqs_client = boto3.client('sqs')
    
    print("Establishing connection to MotherDuck...")
    # Connect to the default MotherDuck environment first
    con = duckdb.connect('md:')
    
    print("Ensuring database and bronze_telemetry table exist...")
    # Create the database if it doesn't exist, then switch to it
    con.execute("CREATE DATABASE IF NOT EXISTS iotstream_db;")
    con.execute("USE iotstream_db;")
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze_telemetry (
            event_id VARCHAR,
            device_id VARCHAR,
            machine_id VARCHAR,
            event_timestamp TIMESTAMP,
            processing_timestamp TIMESTAMP,
            temperature DOUBLE,
            humidity DOUBLE,
            vibration DOUBLE,
            pressure DOUBLE,
            battery INTEGER,
            device_status VARCHAR,
            anomaly_status VARCHAR,
            anomaly_reasons VARCHAR[],
            schema_version INTEGER,
            processing_version VARCHAR
        );
    """)
    
    print("Polling Warehouse Queue for ready events...")
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=5
    )
    
    messages = response.get('Messages', [])
    if not messages:
        print("No events found in the queue. Ingestion complete.")
        return

    print(f"Retrieved {len(messages)} events. Inserting into MotherDuck...")
    
    records = []
    for msg in messages:
        body = json.loads(msg['Body'])
        records.append((
            body['event_id'], body['device_id'], body['machine_id'],
            body['event_timestamp'], body['processing_timestamp'],
            body['temperature'], body['humidity'], body['vibration'],
            body['pressure'], body['battery'], body['device_status'],
            body['anomaly_status'], body['anomaly_reasons'],
            body['schema_version'], body['processing_version']
        ))
        
    # Bulk insert directly into the cloud database
    con.executemany("""
        INSERT INTO bronze_telemetry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    
    print("Data committed to MotherDuck. Cleaning up SQS...")
    for msg in messages:
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=msg['ReceiptHandle']
        )
        
    print("Batch ingestion complete.")

if __name__ == "__main__":
    run_ingestion()