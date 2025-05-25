import boto3
import csv
from io import StringIO
from bs4 import BeautifulSoup
import re
import datetime
import sys

from awsglue.utils import getResolvedOptions

s3 = boto3.client('s3')

# Obtener parámetros pasados al job de Glue
# Asegúrate de que 'S3_BUCKET' y 'SOURCE_DATE' se pasen como argumentos.
args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'SOURCE_DATE'])
S3_BUCKET_NAME = args['S3_BUCKET']
SOURCE_DATE = args['SOURCE_DATE'] # Ej: '2023-10-27'

print(f"Iniciando Job de Glue: Procesar Titulares para fecha: {SOURCE_DATE} en bucket: {S3_BUCKET_NAME}")

def extract_news_data_glue(html_content, source_newspaper):
    """
    CÓDIGO DE EXTRACT_NEWS_DATA (MISMO QUE EN EL LAMBDA)
    Asegúrate de que este código esté sincronizado con src/lambda_functions/process_headlines.py
    y que los SELECTORES CSS/ETIQUETAS estén correctos.
    """
    soup = BeautifulSoup(html_content, 'lxml')
    news_items = []

    if source_newspaper == 'el_tiempo':
        articles = soup.find_all('article', class_='article-card')
        if not articles: articles = soup.find_all('h2', class_='title')

        for article in articles:
            headline_tag = article.find(['h2', 'h3'], class_='title')
            link_tag = article.find('a')
            category_tag = article.find('p', class_='category')

            headline = headline_tag.get_text(strip=True) if headline_tag else 'N/A'
            link = link_tag['href'] if link_tag and 'href' in link_tag.attrs else 'N/A'
            category = category_tag.get_text(strip=True) if category_tag else 'General'

            if category == 'General' and link and '[eltiempo.com/](https://eltiempo.com/)' in link:
                match = re.search(r'eltiempo\.com/(?:noticias|deportes|cultura|economia)/([^/]+)/', link)
                if match: category = match.group(1).replace('-', ' ').title()
                else:
                    match = re.search(r'eltiempo\.com/seccion/([^/]+)/', link)
                    if match: category = match.group(1).replace('-', ' ').title()

            if headline != 'N/A' and link != 'N/A':
                news_items.append({'category': category, 'headline': headline, 'link': link})

    elif source_newspaper == 'el_espectador':
        articles = soup.find_all('div', class_='Card-Body')
        if not articles: articles = soup.find_all('h2', class_='Card-Title')

        for article in articles:
            headline_tag = article.find(['h2', 'h3'], class_='Card-Title')
            link_tag = article.find('a', class_='Card-Link')
            category_tag = article.find('span', class_='Card-Category')

            headline = headline_tag.get_text(strip=True) if headline_tag else 'N/A'
            link = link_tag['href'] if link_tag and 'href' in link_tag.attrs else 'N/A'
            category = category_tag.get_text(strip=True) if category_tag else 'General'

            if category == 'General' and link and '[elespectador.com/](https://elespectador.com/)' in link:
                 match = re.search(r'elespectador\.com/(?:noticias|deportes|cultura|politica|economia)/([^/]+)/', link)
                 if match: category = match.group(1).replace('-', ' ').title()
                 else:
                    match = re.search(r'elespectador\.com/seccion/([^/]+)/', link)
                    if match: category = match.group(1).replace('-', ' ').title()

            if headline != 'N/A' and link != 'N/A':
                news_items.append({'category': category, 'headline': headline, 'link': link})

    return news_items


def process_s3_html_for_glue(date_str):
    """
    Función principal para el Job de Glue que procesa HTML y genera CSVs.
    """
    year, month, day = date_str.split('-')
    newspapers = ['el_tiempo', 'el_espectador'] # Lista de periódicos que descargas

    for name in newspapers:
        raw_s3_key = f'headlines/raw/{name}-contenido-{date_str}.html'
        print(f"Intentando procesar archivo raw: {raw_s3_key}")

        try:
            obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=raw_s3_key)
            html_content = obj['Body'].read().decode('utf-8')

            extracted_data = extract_news_data_glue(html_content, name)

            if not extracted_data:
                print(f"No se extrajo información de noticias de {raw_s3_key}. Saltando creación de CSV.")
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

            print(f"Subiendo CSV a s3://{S3_BUCKET_NAME}/{csv_s3_key}...")
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=csv_s3_key, Body=csv_content.encode('utf-8'), ContentType='text/csv')
            print(f"Procesado exitosamente {raw_s3_key} y subido CSV.")

        except s3.exceptions.NoSuchKey:
            print(f"Archivo raw {raw_s3_key} no encontrado. Saltando procesamiento para este periódico en esta fecha.")
        except Exception as e:
            print(f"Error procesando {raw_s3_key}: {e}")

    print("Procesamiento HTML y carga de CSV completados para Job de Glue 2.")

if __name__ == '__main__':
    process_s3_html_for_glue(SOURCE_DATE)