# process_headlines_glue_job.py
import boto3
import csv
from io import StringIO
from bs4 import BeautifulSoup
import re
import datetime
import sys

from awsglue.utils import getResolvedOptions

# Initialize S3 client
s3 = boto3.client('s3')

# Get parameters passed to the Glue job
args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'SOURCE_DATE'])
S3_BUCKET_NAME = args['S3_BUCKET']
SOURCE_DATE = args['SOURCE_DATE'] # e.g., '2023-10-27'

print(f"Starting Glue Job: Process Headlines for date: {SOURCE_DATE} in bucket: {S3_BUCKET_NAME}")

def extract_news_data(html_content, source_newspaper):
    # Your Beautiful Soup parsing logic as in the Lambda, potentially refined.
    # THIS PART IS CRITICAL AND REQUIRES INSPECTING LIVE HTML.
    # I'm providing the same example structure, you will need to verify/adapt.
    soup = BeautifulSoup(html_content, 'html.parser')
    news_items = []

    if source_newspaper == 'el_tiempo':
        articles = soup.find_all('article', class_='article-card')
        for article in articles:
            headline_tag = article.find(['h2', 'h3'], class_='title')
            link_tag = article.find('a')
            category_tag = article.find('p', class_='category')

            headline = headline_tag.get_text(strip=True) if headline_tag else 'N/A'
            link = link_tag['href'] if link_tag and 'href' in link_tag.attrs else 'N/A'
            category = category_tag.get_text(strip=True) if category_tag else 'General'

            if category == 'General' and link and 'eltiempo.com/' in link:
                match = re.search(r'eltiempo\.com/noticias/([^/]+)/', link)
                if match:
                    category = match.group(1).replace('-', ' ').title()

            if headline != 'N/A' and link != 'N/A': # Only add if essential data is found
                news_items.append({
                    'category': category,
                    'headline': headline,
                    'link': link
                })
    elif source_newspaper == 'el_espectador':
        articles = soup.find_all('div', class_='Card-Body')
        for article in articles:
            headline_tag = article.find(['h2', 'h3'], class_='Card-Title')
            link_tag = article.find('a', class_='Card-Link')
            category_tag = article.find('span', class_='Card-Category')

            headline = headline_tag.get_text(strip=True) if headline_tag else 'N/A'
            link = link_tag['href'] if link_tag and 'href' in link_tag.attrs else 'N/A'
            category = category_tag.get_text(strip=True) if category_tag else 'General'

            if category == 'General' and link and 'elespectador.com/' in link:
                 match = re.search(r'elespectador\.com/(?:noticias|deportes|cultura)/([^/]+)/', link) # Added more paths
                 if match:
                    category = match.group(1).replace('-', ' ').title()

            if headline != 'N/A' and link != 'N/A':
                news_items.append({
                    'category': category,
                    'headline': headline,
                    'link': link
                })

    return news_items

def process_s3_html_for_glue(date_str):
    year, month, day = date_str.split('-')
    newspapers = ['el_tiempo', 'el_espectador'] # List of newspapers you download

    for name in newspapers:
        raw_s3_key = f'headlines/raw/{name}-contenido-{date_str}.html'
        print(f"Attempting to process raw file: {raw_s3_key}")

        try:
            obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=raw_s3_key)
            html_content = obj['Body'].read().decode('utf-8')

            extracted_data = extract_news_data(html_content, name)

            if not extracted_data:
                print(f"No news data extracted from {raw_s3_key}. Skipping CSV creation.")
                continue

            csv_buffer = StringIO()
            fieldnames = ['category', 'headline', 'link']
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
            writer.writeheader()
            for item in extracted_data:
                writer.writerow(item)

            csv_content = csv_buffer.getvalue()

            csv_s3_key = (f'headlines/final/periodico={name}/year={year}/'
                          f'month={month}/day={day}/{name}_headlines.csv')

            print(f"Uploading CSV to s3://{S3_BUCKET_NAME}/{csv_s3_key}...")
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=csv_s3_key, Body=csv_content, ContentType='text/csv')
            print(f"Successfully processed {raw_s3_key} and uploaded CSV.")

        except s3.exceptions.NoSuchKey:
            print(f"Raw file {raw_s3_key} not found. Skipping processing for this newspaper on this date.")
        except Exception as e:
            print(f"Error processing {raw_s3_key}: {e}")

    print("HTML processing and CSV upload completed for Glue Job 2.")

if __name__ == '__main__':
    process_s3_html_for_glue(SOURCE_DATE)