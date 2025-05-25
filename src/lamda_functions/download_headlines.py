import requests
import boto3
import datetime
import os
import sys

# Initialize S3 client
s3 = boto3.client('s3', region_name='us-east-1')

# Get S3 bucket name from environment variable
# IMPORTANT: This will be set by Zappa's environment_variables in zappa_settings.json
S3_BUCKET_NAME = os.environ.get('S3_BUCKET', 'your-default-bucket-if-not-set')

def download_and_upload_to_s3(event, context):
    """
    Lambda function to download news headlines and upload raw HTML to S3.
    Triggered on a schedule (e.g., daily).
    """
    today = datetime.date.today()
    date_str = today.strftime('%Y-%m-%d')

    newspapers = {
        'el_tiempo': '[https://www.eltiempo.com/](https://www.eltiempo.com/)',
        'el_espectador': '[https://www.elespectador.com/](https://www.elespectador.com/)',
    }

    print(f"Starting download and upload for date: {date_str} to bucket: {S3_BUCKET_NAME}")

    for name, url in newspapers.items():
        try:
            print(f"Attempting to download {url}...")
            response = requests.get(url, timeout=15) # Increased timeout
            response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
            content = response.text

            s3_key = f'headlines/raw/{name}-contenido-{date_str}.html'

            print(f"Uploading {name} content to s3://{S3_BUCKET_NAME}/{s3_key}...")
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=content.encode('utf-8'), ContentType='text/html')
            print(f"Successfully uploaded {name} content.")

        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {e}")
            # Optionally, log to a different system or trigger an alert
        except boto3.exceptions.S3UploadFailedError as e:
            print(f"Error uploading to S3 for {name} ({url}): {e}")
        except Exception as e:
            print(f"An unexpected error occurred for {name} ({url}): {e}")

    print("Download and upload process completed.")

    return {
        'statusCode': 200,
        'body': 'Download and upload process completed.'
    }