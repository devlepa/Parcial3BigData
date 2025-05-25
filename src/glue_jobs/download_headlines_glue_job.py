# download_headlines_glue_job.py
import requests
import boto3
import datetime
import os
import sys

# For Glue jobs, arguments are passed via sys.argv or Glue's getResolvedOptions
from awsglue.utils import getResolvedOptions

# Initialize S3 client
s3 = boto3.client('s3')

# Get parameters passed to the Glue job
args = getResolvedOptions(sys.argv, ['S3_BUCKET'])
S3_BUCKET_NAME = args['S3_BUCKET']

print(f"Starting Glue Job: Download Headlines to S3 bucket: {S3_BUCKET_NAME}")

def download_and_upload_to_s3_glue():
    today = datetime.date.today()
    date_str = today.strftime('%Y-%m-%d')

    newspapers = {
        'el_tiempo': 'https://www.eltiempo.com/',
        'el_espectador': 'https://www.elespectador.com/',
        # 'publimetro': 'https://www.publimetro.co/',
    }

    for name, url in newspapers.items():
        try:
            print(f"Attempting to download {url}...")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            content = response.text

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

    print("Download and upload process completed for Glue Job 1.")

if __name__ == '__main__':
    download_and_upload_to_s3_glue()