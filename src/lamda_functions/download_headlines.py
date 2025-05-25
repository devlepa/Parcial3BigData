import requests
import boto3
import datetime
import os

s3 = boto3.client('s3', region_name='us-east-1')

# Define your S3 bucket name
# It's good practice to get this from environment variables or Zappa settings
# For now, you can hardcode it or use an environment variable (recommended for Zappa)

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'parcial3bigdata000')

def download_and_upload_to_s3(event, context):
    """
    Lambda function to download newspaper homepages and upload them to S3.
    Triggered by a CloudWatch scheduled event (daily).
    """
    today = datetime.date.today()
    date_str = today.strftime('%Y-%m-%d')

    newspapers = {
        'el_tiempo': 'https://www.eltiempo.com/',
        'el_espectador': 'https://www.elespectador.com/',
    }

    for name, url in newspapers.items():
        try:
            print(f"Attempting to download {url}...")
            response = requests.get(url, timeout=10) # Added timeout for robustness
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            content = response.text

            # S3 key structure: s3://bucket/headlines/raw/contenido-yyyy-mm-dd.html
            s3_key = f'headlines/raw/{name}-contenido-{date_str}.html'

            print(f"Uploading {name} content to s3://{S3_BUCKET_NAME}/{s3_key}...")
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=content, ContentType='text/html')
            print(f"Successfully uploaded {name} content.")

        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {e}")
        except boto3.exceptions.S3UploadFailedError as e:
            print(f"Error uploading to S3 for {name}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred for {name}: {e}")

    return {
        'statusCode': 200,
        'body': 'Download and upload process completed.'
    }

# This part is crucial for Zappa to find your function when deploying
# Zappa typically looks for a WSGI app or a direct function reference.
# For a scheduled event, you directly reference the function.
# If you were deploying a Flask/Django app, it would be 'app'
# For a raw lambda function, it's the function itself.
# No need to wrap it in Flask/Django for scheduled tasks with Zappa.