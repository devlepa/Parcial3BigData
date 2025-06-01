# src/lambda_functions/process_headlines.py
import os
import re
from urllib.parse import unquote_plus
import pandas as pd
from bs4 import BeautifulSoup
import boto3
import json

s3_client = boto3.client('s3')

def process_s3_html_event(event, context):
    print(f"Received event: {json.dumps(event)}")

    s3_data_bucket = os.environ.get('S3_DATA_BUCKET')
    if not s3_data_bucket:
        print("ERROR: S3_DATA_BUCKET environment variable not set.")
        return {
            'statusCode': 500,
            'body': 'S3_DATA_BUCKET environment variable not configured.'
        }

    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        if not key.startswith('headlines/raw/') or not key.endswith('.html'):
            print(f"Skipping non-headline HTML object or wrong path: {key}")
            continue

        try:
            print(f"Processing object: s3://{bucket_name}/{key}")
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            html_content = response['Body'].read().decode('utf-8')
            print(f"Successfully downloaded {key} from {bucket_name}")

            soup = BeautifulSoup(html_content, 'html.parser')
            headlines_data = []

            # =========================================================================
            # Lógica adaptada directamente de tu scraping_espectador.py
            # =========================================================================
            cards = soup.find_all("div", class_="BlockContainer-Content")
            if not cards:
                print(f"WARNING: No 'BlockContainer-Content' cards found in {key}.")

            for card in cards:
                category_tag = card.find("div", class_="Card-SectionContainer")
                category = category_tag.get_text(strip=True) if category_tag else "No category"

                title_tag = card.find("h2", class_="Card-Title")
                title = title_tag.get_text(strip=True) if title_tag else "No title"

                description_tag = card.find("div", class_="Card-Hook")
                description = description_tag.get_text(strip=True) if description_tag else "No description"

                # Opcional: También podrías extraer el link si lo necesitas para el CSV
                # link_tag = card.find("a", class_="Card-Title") # O la clase de tu link
                # link = link_tag['href'] if link_tag and 'href' in link_tag.attrs else None

                if title != "No title": # Asegurarse de que al menos haya un título
                    headlines_data.append({
                        "category": category,
                        "title": title,
                        "description": description
                        # "link": link # Si lo quieres añadir
                    })
            # =========================================================================

            if not headlines_data:
                print(f"WARNING: No valid headlines extracted from {key}. Output CSV will be empty or not created.")
                # Si no hay datos, podrías decidir no crear el archivo CSV o crear uno vacío
                return

            df = pd.DataFrame(headlines_data)

            # Define la nueva clave S3 para el archivo CSV procesado
            output_csv_key = key.replace('headlines/raw/', 'headlines/processed/').replace('.html', '.csv')

            csv_buffer = df.to_csv(index=False, encoding='utf-8')

            s3_client.put_object(
                Bucket=s3_data_bucket,
                Key=output_csv_key,
                Body=csv_buffer,
                ContentType='text/csv'
            )
            print(f"Successfully uploaded processed CSV to s3://{s3_data_bucket}/{output_csv_key}")

        except Exception as e:
            print(f"Error processing {key}: {e}")
            import traceback
            traceback.print_exc()
            raise e

    return {
        'statusCode': 200,
        'body': json.dumps('HTML processing complete.')
    }