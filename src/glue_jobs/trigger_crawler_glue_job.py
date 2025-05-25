# trigger_crawler_glue_job.py
import boto3
import sys

from awsglue.utils import getResolvedOptions

# Initialize Glue client
glue = boto3.client('glue')

# Get parameters passed to the Glue job
args = getResolvedOptions(sys.argv, ['GLUE_CRAWLER_NAME'])
GLUE_CRAWLER_NAME = args['GLUE_CRAWLER_NAME']

print(f"Starting Glue Job: Trigger Crawler for {GLUE_CRAWLER_NAME}")

def trigger_glue_crawler_glue():
    print(f"Attempting to start Glue crawler: {GLUE_CRAWLER_NAME}")
    try:
        response = glue.start_crawler(Name=GLUE_CRAWLER_NAME)
        print(f"Successfully started Glue crawler: {GLUE_CRAWLER_NAME}. Response: {response}")
    except glue.exceptions.CrawlerRunningException:
        print(f"Crawler {GLUE_CRAWLER_NAME} is already running. Skipping.")
    except Exception as e:
        print(f"Error starting Glue crawler {GLUE_CRAWLER_NAME}: {e}")
        raise # Re-raise to fail the job if the crawler cannot be started

    print("Glue Crawler trigger process completed for Glue Job 3.")

if __name__ == '__main__':
    trigger_glue_crawler_glue()