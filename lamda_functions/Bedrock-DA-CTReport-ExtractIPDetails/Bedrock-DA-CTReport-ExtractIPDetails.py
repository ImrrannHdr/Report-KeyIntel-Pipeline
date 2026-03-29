import json
import boto3
import urllib.parse

glue = boto3.client('glue')

GLUE_JOB_NAME = "Bedrock-DA-CTReport-StructureData-IPEnrich"
OUTPUT_PREFIX = "enriched"
IPINFO_TOKEN = "0f8f09070fec09"

def lambda_handler(event, context):
    try:
        # Get S3 event info
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        object_key = urllib.parse.unquote_plus(record['s3']['object']['key'])

        print(f"Triggered by file: s3://{bucket_name}/{object_key}")

        # Start Glue Job
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--INPUT_BUCKET': bucket_name,
                '--INPUT_KEY': object_key,
                '--OUTPUT_PREFIX': OUTPUT_PREFIX,
                '--IPINFO_TOKEN': IPINFO_TOKEN
            }
        )

        print("Glue job started:", response['JobRunId'])

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Glue job started successfully',
                'jobRunId': response['JobRunId'],
                'input_bucket': bucket_name,
                'input_key': object_key
            })
        }

    except Exception as e:
        print("Error starting Glue job:", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to start Glue job',
                'error': str(e)
            })
        }
