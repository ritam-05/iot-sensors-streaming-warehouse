import os
import json
import boto3
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Support both Lambda's package import and direct local execution of this file.
try:
    from src.generator.simulator import IoTSimulator
    from src.generator.schema import TelemetryEvent
except ModuleNotFoundError:
    from simulator import IoTSimulator
    from schema import TelemetryEvent

# Initialize the Boto3 client outside the handler for warm-start performance
sqs_client = boto3.client('sqs')

def lambda_handler(event, context):
    """
    AWS Lambda entry point triggered by EventBridge Scheduler.
    """
    # We will set this environment variable later during infrastructure deployment
    queue_url = os.environ.get("TELEMETRY_QUEUE_URL")
    
    print("Initializing IoT Simulator...")
    simulator = IoTSimulator()
    raw_events = simulator.generate_batch()
    
    valid_events = []
    
    # Phase 3 Integration: Validate all events
    for raw_event in raw_events:
        try:
            validated_event = TelemetryEvent(**raw_event)
            valid_events.append(validated_event)
        except Exception as e:
            print(f"Validation failed for event {raw_event.get('event_id')}: {e}")
            
    if not valid_events:
        print("No valid events to send.")
        return {"statusCode": 400, "body": "No valid events generated."}

    # Prepare SQS Batch entries (Max 10 messages per batch in AWS SQS)
    entries = []
    for evt in valid_events:
        entries.append({
            'Id': evt.event_id,
            'MessageBody': evt.model_dump_json()
        })
        
    print(f"Prepared {len(entries)} valid events for SQS.")

    # Local testing bypass
    if not queue_url:
        print("\n[LOCAL MODE] TELEMETRY_QUEUE_URL is not set.")
        print("Simulating SQS batch send locally:\n")
        print(json.dumps([json.loads(e['MessageBody']) for e in entries], indent=2))
        return {"statusCode": 200, "body": "Executed locally without SQS."}

    # Send to AWS SQS
    try:
        response = sqs_client.send_message_batch(
            QueueUrl=queue_url,
            Entries=entries
        )
        
        # SQS Batch partial failure handling
        if 'Failed' in response and response['Failed']:
            print(f"Failed to send {len(response['Failed'])} messages: {response['Failed']}")
            # Raising an exception forces Lambda to report failure, triggering EventBridge retries
            raise RuntimeError(f"Partial batch failure in SQS submission.")
        else:
            print(f"Successfully sent {len(entries)} events to SQS.")
            
        return {"statusCode": 200, "body": "Successfully sent to SQS"}
    
    except Exception as e:
        print(f"Error sending batch to SQS: {e}")
        raise e

# Local testing block
if __name__ == "__main__":
    lambda_handler({}, None)
