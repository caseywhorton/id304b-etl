import json
import boto3

def lambda_handler(event, context):
    glue_client = boto3.client('glue')
    s3_client = boto3.client('s3')

    print("Received event:", json.dumps(event))
    
    # Extract S3 bucket and file details from EventBridge format
    if 'detail' in event:
        # This is an EventBridge event
        bucket_name = event['detail']['bucket']['name']
        file_key = event['detail']['object']['key']
    elif 'Records' in event:
        # This is a direct S3 notification (just in case)
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
    else:
        print("Unknown event format:", event)
        return {
            'statusCode': 400,
            'body': json.dumps("Unknown event format")
        }

    print('received s3 item')
    print('bucket_name: ', bucket_name)
    print('file_key: ', file_key)
    
    # Glue Job Name
    job_name = "ID304B-ETL"  # Replace with your Glue job name

    # Pass S3 details to Glue Job using Glue Job Arguments
    job_arguments = {
        '--bucket': bucket_name,
        '--key': file_key
    }

    try:
        # Start the Glue job and pass the arguments
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=job_arguments
        )
        print(f"Glue Job Started: {response['JobRunId']}")
        return {
            'statusCode': 200,
            'body': json.dumps(f"Glue job {job_name} started successfully with file {file_key}")
        }
    except Exception as e:
        print(f"Error starting Glue job: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Failed to start Glue job: {str(e)}")
        }
