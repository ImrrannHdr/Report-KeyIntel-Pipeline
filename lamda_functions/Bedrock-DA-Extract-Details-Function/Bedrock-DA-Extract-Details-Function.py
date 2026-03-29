import json
import boto3
import urllib.parse

bedrock = boto3.client('bedrock-data-automation-runtime', region_name='us-east-1')

PROJECT_ARN = "arn:aws:bedrock:us-east-1:604842892210:data-automation-project/f26c8a888a6d"
OUTPUT_BUCKET = "bedrock-daut-cybertiplinereport-details-output"
DATA_AUTOMATION_PROFILE_ARN = "arn:aws:bedrock:us-east-1:604842892210:data-automation-profile/us.data-automation-v1"

def lambda_handler(event, context):
    try:
        # 1. Extract S3 details from event
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])

        input_s3_uri = f"s3://{bucket}/{key}"
        output_s3_uri = f"s3://{OUTPUT_BUCKET}/bedrock-output/{key}.json"

        
        response = bedrock.invoke_data_automation_async(
            inputConfiguration={
                's3Uri': input_s3_uri
            },
            outputConfiguration={
                's3Uri': output_s3_uri
            },
            dataAutomationConfiguration={
                'dataAutomationProjectArn': PROJECT_ARN,
                'stage': 'LIVE'
            },
            dataAutomationProfileArn=DATA_AUTOMATION_PROFILE_ARN
            
        )

        invocation_arn = response.get("invocationArn")
        print(f"SUCCESS - Invocation ARN: {invocation_arn}")
        print(f"Input: {input_s3_uri}")
        print(f"Output: {output_s3_uri}")
        print(response)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Bedrock job started successfully",
                "invocationArn": invocation_arn,
                "outputLocation": output_s3_uri
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        raise e