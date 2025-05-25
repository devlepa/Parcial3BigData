import boto3
import csv
from io import StringIO
from urllib.parse import unquote_plus
from bs4 import BeautifulSoup
import re
import datetime
import os
import sys
from dotenv import load_dotenv
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# Carga variables de entorno del archivo .env si existe (solo para desarrollo local)
load_dotenv()

# Inicializa GlueContext para Glue Job
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Obtiene argumentos del Job de Glue.
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_DATA_BUCKET', 'SOURCE_KEY_PREFIX'])
    S3_BUCKET_NAME = args['S3_DATA_BUCKET']
    SOURCE_KEY_PREFIX = args['SOURCE_KEY_PREFIX'] # Ej: headlines/raw/
except Exception as e:
    print(f"Advertencia: No se pudieron obtener argumentos de Glue Job. Intentando leer de variables de entorno. Error: {e}")
    S3_BUCKET_NAME = os.environ.get('S3_DATA_BUCKET')
    SOURCE_KEY_PREFIX = os.environ.get('SOURCE_KEY_PREFIX', 'headlines/raw/') # Fallback

if not S3_BUCKET_NAME:
    raise ValueError("S3_DATA_BUCKET no está configurado.")

s3 = boto3.client('s3')

def extract_news_data_glue(html_content, source_name):
    """
    Extrae titulares, enlaces y categorías del contenido HTML.
    Adaptado para Glue Job.
    
    ¡IMPORTANTE! Los selectores CSS ('article', 'h2.title', etc.) son EJEMPLOS.
    DEBES inspeccionar el HTML de eltiempo.com y elespectador.com para encontrar
    los selectores EXACTOS que te permitan extraer los datos correctamente.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    news_items = []

    if source_name == "eltiempo":
        # EJEMPLO: Selector para El Tiempo. ¡AJUSTAR!
        articles = soup.find_all('article', class_=lambda x: x and ('story-card' in x or 'listing-card' in x))
        for article in articles:
            title_tag = article.find(['h2', 'h3'], class_=lambda x: x and ('article-title' in x or 'title' in x))
            link_tag = article.find('a', class_=lambda x: x and ('title' in x or 'story-link' in x))
            category_tag = article.find('span', class_=lambda x: x and ('category' in x or 'section-name' in x))

            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            link = "https://www.eltiempo.com" + link_tag['href'] if link_tag and link_tag.get('href') else "N/A"
            category = category_tag.get_text(strip=True) if category_tag else "N/A"
            
            if title != "N/A" and link != "N/A":
                news_items.append({
                    "categoria": category,
                    "titular": title,
                    "enlace": link,
                    "fecha_extraccion": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
    elif source_name == "elespectador":
        # EJEMPLO: Selector para El Espectador. ¡AJUSTAR!
        articles = soup.find_all('article', class_=lambda x: x and ('Card-Container' in x))
        for article in articles:
            title_tag = article.find('h2', class_=lambda x: x and 'Card-Title' in x)
            link_tag = article.find('a', class_=lambda x: x and 'Card-Title' in x)
            category_tag = article.find('span', class_=lambda x: x and 'Card-Category' in x)

            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            link = link_tag['href'] if link_tag and link_tag.get('href') else "N/A"
            category = category_tag.get_text(strip=True) if category_tag else "N/A"

            if title != "N/A" and link != "N/A":
                news_items.append({
                    "categoria": category,
                    "titular": title,
                    "enlace": link,
                    "fecha_extraccion": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
    else:
        print(f"Fuente de noticias desconocida: {source_name}")

    return news_items

def process_html_files_from_s3_glue():
    """
    Función principal para el Glue Job que procesa archivos HTML de S3.
    Escanea el prefijo `headlines/raw/` y procesa los nuevos archivos.
    """
    print(f"Iniciando procesamiento de archivos HTML de S3 como Glue Job. Bucket: {S3_BUCKET_NAME}, Prefijo: {SOURCE_KEY_PREFIX}")

    # Lista todos los objetos en el prefijo de entrada
    objects_to_process = []
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=SOURCE_KEY_PREFIX)

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.html'):
                    objects_to_process.append(key)

    if not objects_to_process:
        print(f"No se encontraron archivos HTML en s3://{S3_BUCKET_NAME}/{SOURCE_KEY_PREFIX} para procesar.")
        return

    for key in objects_to_process:
        print(f"Procesando el archivo S3: {key}")
        try:
            # Descargar el archivo HTML
            response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
            html_content = response['Body'].read().decode('utf-8')

            # Extraer el nombre de la fuente (ej: eltiempo, elespectador)
            path_parts = key.split('/')
            source_name = path_parts[2] if len(path_parts) > 2 else "unknown"
            print(f"Fuente de noticias detectada: {source_name}")

            # Extraer datos de noticias
            news_data = extract_news_data_glue(html_content, source_name)
            
            if not news_data:
                print(f"No se pudieron extraer datos de noticias del archivo: {key}.")
                continue

            # Preparar datos para CSV
            output_buffer = StringIO()
            fieldnames = ["categoria", "titular", "enlace", "fecha_extraccion"]
            writer = csv.DictWriter(output_buffer, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            writer.writerows(news_data)

            # Determinar el path de salida particionado
            date_part = key.split('/')[-2]
            if re.match(r'\d{4}-\d{2}-\d{2}', date_part):
                year, month, day = date_part.split('-')
            else:
                now = datetime.datetime.now()
                year = now.strftime("%Y")
                month = now.strftime("%m")
                day = now.strftime("%d")
                print(f"Advertencia: No se pudo extraer la fecha del path '{key}'. Usando fecha actual.")

            output_key = (
                f"headlines/final/"
                f"periodico={source_name}/"
                f"year={year}/"
                f"month={month}/"
                f"day={day}/"
                f"{source_name}_{date_part if re.match(r'\d{4}-\d{2}-\d{2}', date_part) else datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
            )

            # Subir el CSV a S3
            print(f"Subiendo datos CSV procesados a s3://{S3_BUCKET_NAME}/{output_key}")
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=output_key, Body=output_buffer.getvalue())
            print(f"Archivo {output_key} subido exitosamente.")

        except Exception as e:
            print(f"Error al procesar el archivo {key}: {e}")
            # Considera marcar el Job como fallido o re-lanzar si es un error crítico
            # raise e # Descomentar para que el Job falle si hay un error en un archivo

if __name__ == '__main__':
    job.init(args['JOB_NAME'], args)
    process_html_files_from_s3_glue()
    job.commit()