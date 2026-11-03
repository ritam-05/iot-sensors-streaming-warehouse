import os
import json
import boto3
from dotenv import load_dotenv

load_dotenv()

# Support both Lambda environment and local execution
try:
    from src.etl.transformer import ETLTransformer
    from src.etl.dynamodb_client import DynamoDBStateStore
except ModuleNotFoundError:
    from transformer import ETLTransformer
    from dynamodb_client import DynamoDBStateStore

# Initialize clients outside the handler for warm-start performance
sqs_client = boto3.client('sqs')
transformer = ETLTransformer()
dynamodb_store = DynamoDBStateStore()

def lambda_handler(event, context):
    """
    Triggered by SQS. Processes a batch of telemetry events.
    """
    warehouse_queue_url = os.environ.get("WAREHOUSE_QUEUE_URL")
    records = event.get('Records', [])
    
    successful_events = []
    
    for record in records:
        try:
            # 1. Extract
            raw_payload = json.loads(record['body'])
            
            # 2. Transform
            processed_event = transformer.transform(raw_payload)
            
            # 3. Load - Operational (DynamoDB)
            dynamodb_store.update_state(processed_event)
            
            successful_events.append(processed_event)
            
        except Exception as e:
            print(f"Error processing record {record.get('messageId')}: {e}")
            # Raising the error tells SQS this specific Lambda invocation failed
            # SQS will retry, and eventually send the message to the DLQ
            raise e 
            
    # 4. Load - Analytical (Warehouse SQS)
    if successful_events and warehouse_queue_url:
        entries = []
        for evt in successful_events:
            entries.append({
                'Id': evt['event_id'],
                'MessageBody': json.dumps(evt)
            })
            
        # SQS Batch API handles maximum 10 messages at a time
        for i in range(0, len(entries), 10):
            batch = entries[i:i+10]
            try:
                sqs_client.send_message_batch(
                    QueueUrl=warehouse_queue_url,
                    Entries=batch
                )
            except Exception as e:
                print(f"Failed to route batch to Warehouse Queue: {e}")
                raise e
                
        print(f"Successfully routed {len(successful_events)} enriched events to Warehouse Queue.")
        
    return {"statusCode": 200, "body": f"Processed {len(successful_events)} events"}

# Local execution block to drain the Telemetry Queue
if __name__ == "__main__":
    telemetry_queue_url = os.environ.get("TELEMETRY_QUEUE_URL")
    
    print(f"Polling Telemetry Queue for messages...")
    response = sqs_client.receive_message(
        QueueUrl=telemetry_queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=5
    )
    
    messages = response.get('Messages', [])
    
    if not messages:
        print("No messages found in the queue.")
    else:
        print(f"Found {len(messages)} raw messages. Processing through ETL pipeline...")
        
        # Mock the AWS Lambda event structure
        mock_event = {'Records': []}
        for msg in messages:
            mock_event['Records'].append({
                'messageId': msg['MessageId'],
                'body': msg['Body'],
                'receiptHandle': msg['ReceiptHandle']
            })
            
        # Execute the handler
        lambda_handler(mock_event, None)
        
        # Delete messages from the source queue so they aren't processed again
        print("Cleaning up processed messages from Telemetry Queue...")
        for msg in messages:
            sqs_client.delete_message(
                QueueUrl=telemetry_queue_url,
                ReceiptHandle=msg['ReceiptHandle']
            )
            
        print("ETL Pipeline execution complete!")