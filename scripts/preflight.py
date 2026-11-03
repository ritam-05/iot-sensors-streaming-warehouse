import boto3
import sys
from botocore.exceptions import ClientError

def check_aws_identity():
    """Verifies the configured AWS identity."""
    print("Checking AWS Credentials...")
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"✅ Authenticated successfully.")
        print(f"   Account ID: {identity['Account']}")
        print(f"   User ARN:   {identity['Arn']}")
        return True
    except ClientError as e:
        print(f"❌ Authentication failed: {e}")
        print("Please run 'aws configure' to set up your credentials.")
        return False

def check_service_access(service_name, client_method, **kwargs):
    """Attempts a safe read-only operation to verify SCP/IAM permissions."""
    print(f"Checking {service_name} access...")
    try:
        client = boto3.client(service_name)
        method = getattr(client, client_method)
        method(**kwargs)
        print(f"✅ {service_name} access permitted.")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['AccessDeniedException', 'AccessDenied']:
            print(f"❌ {service_name} access DENIED by IAM or SCP.")
        else:
            print(f"⚠️ {service_name} check returned an unexpected error: {error_code}")
        return False

def main():
    print("=== IoTStream AWS Preflight Check ===\n")
    
    if not check_aws_identity():
        sys.exit(1)
        
    print("\n--- Verifying Core Services ---")
    
    # Check Lambda (List functions)
    check_service_access('lambda', 'list_functions', MaxItems=1)
    
    # Check SQS (List queues)
    check_service_access('sqs', 'list_queues', MaxResults=1)
    
    # Check DynamoDB (List tables)
    check_service_access('dynamodb', 'list_tables', Limit=1)
    
    # Check EventBridge/Events (List rules)
    check_service_access('events', 'list_rules', Limit=1)
    
    # Check IAM (List roles - crucial for SAM deployments)
    check_service_access('iam', 'list_roles', MaxItems=1)

    print("\nPreflight complete. If any core services were denied, you must contact your AWS Organization admin to request access before we proceed to infrastructure deployment.")

if __name__ == "__main__":
    main()