# src/lambda_functions/trigger_crawler.py
import os
import boto3
import json

glue_client = boto3.client('glue')

def trigger_glue_crawler(event, context):
    print("Starting trigger_glue_crawler Lambda function.")

    crawler_name = os.environ.get('GLUE_CRAWLER_NAME')
    if not crawler_name:
        print("ERROR: GLUE_CRAWLER_NAME environment variable not set.")
        return {
            'statusCode': 500,
            'body': 'GLUE_CRAWLER_NAME not configured.'
        }

    try:
        crawler_status_response = glue_client.get_crawler(Name=crawler_name)
        crawler_state = crawler_status_response['Crawler']['State']
        print(f"Crawler '{crawler_name}' current state: {crawler_state}")

        if crawler_state not in ['RUNNING', 'STOPPING']:
            print(f"Attempting to start Glue Crawler: {crawler_name}")
            response = glue_client.start_crawler(Name=crawler_name)
            print(f"Successfully initiated Glue Crawler: {crawler_name}")
            print(f"Response: {json.dumps(response)}")
            return {
                'statusCode': 200,
                'body': f'Glue Crawler {crawler_name} started successfully.'
            }
        else:
            print(f"Crawler '{crawler_name}' is already {crawler_state}. Skipping start.")
            return {
                'statusCode': 200,
                'body': f'Glue Crawler {crawler_name} is already {crawler_state}.'
            }

    except Exception as e:
        print(f"Error starting Glue Crawler {crawler_name}: {e}")
        import traceback
        traceback.print_exc()
        raise e