import boto3
import os

# Initialize Glue client
glue = boto3.client('glue')

# Get the crawler name from environment variables or hardcode (environment variable preferred for Zappa)
GLUE_CRAWLER_NAME = os.environ.get('GLUE_CRAWLER_NAME', 'headlines_csv_crawler') # Use the name you gave your crawler

def trigger_glue_crawler(event, context):
    """
    Lambda function to trigger an AWS Glue crawler.
    This function can be invoked manually, via CloudWatch event, or by another Lambda.
    For this partial, it's typically invoked on demand or after the CSV is generated (though
    the requirement states "Cree un tercer lambda que ejecute un crawler", implying a direct trigger,
    not necessarily tied to the S3 event).
    """
    print(f"Attempting to start Glue crawler: {GLUE_CRAWLER_NAME}")
    try:
        response = glue.start_crawler(Name=GLUE_CRAWLER_NAME)
        print(f"Successfully started Glue crawler: {GLUE_CRAWLER_NAME}. Response: {response}")
        return {
            'statusCode': 200,
            'body': f'Successfully started Glue crawler: {GLUE_CRAWLER_NAME}'
        }
    except glue.exceptions.CrawlerRunningException:
        print(f"Crawler {GLUE_CRAWLER_NAME} is already running. Skipping.")
        return {
            'statusCode': 200,
            'body': f'Crawler {GLUE_CRAWLER_NAME} is already running.'
        }
    except Exception as e:
        print(f"Error starting Glue crawler {GLUE_CRAWLER_NAME}: {e}")
        return {
            'statusCode': 500,
            'body': f'Error starting Glue crawler {GLUE_CRAWLER_NAME}: {e}'
        }