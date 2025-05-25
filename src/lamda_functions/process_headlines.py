import boto3
import csv
from io import StringIO
from urllib.parse import unquote_plus
from bs4 import BeautifulSoup
import re
import datetime
import os
from dotenv import load_dotenv

# Carga variables de entorno del archivo .env si existe (solo para desarrollo local)
load_dotenv()

s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION', 'us-east-1'))

# Obtiene el nombre del bucket S3 de las variables de entorno
S3_BUCKET_NAME = os.environ.get('S3_DATA_BUCKET')

def extract_news_data(html_content, source_name):
    """
    Extrae titulares, enlaces y categorías del contenido HTML.
    
    ¡IMPORTANTE! Los selectores CSS ('article', 'h2.title', etc.) son EJEMPLOS.
    DEBES inspeccionar el HTML de eltiempo.com y elespectador.com para encontrar
    los selectores EXACTOS que te permitan extraer los datos correctamente.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    news_items = []

    if source_name == "eltiempo":
        # EJEMPLO: Selector para El Tiempo. ¡AJUSTAR!
        # Busca un contenedor general de noticias, luego los elementos dentro
        articles = soup.find_all('article', class_=lambda x: x and ('story-card' in x or 'listing-card' in x))
        for article in articles:
            title_tag = article.find(['h2', 'h3'], class_=lambda x: x and ('article-title' in x or 'title' in x))
            link_tag = article.find('a', class_=lambda x: x and ('title' in x or 'story-link' in x))
            category_tag = article.find('span', class_=lambda x: x and ('category' in x or 'section-name' in x))

            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            link = "https://www.eltiempo.com" + link_tag['href'] if link_tag and link_tag.get('href') else "N/A"
            category = category_tag.get_text(strip=True) if category_tag else "N/A"
            
            if title != "N/A" and link != "N/A": # Filtra entradas inválidas
                news_items.append({
                    "categoria": category,
                    "titular": title,
                    "enlace": link,
                    "fecha_extraccion": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
    elif source_name == "elespectador":
        # EJEMPLO: Selector para El Espectador. ¡AJUSTAR!
        # Busca un contenedor general de noticias, luego los elementos dentro
        articles = soup.find_all('article', class_=lambda x: x and ('Card-Container' in x))
        for article in articles:
            title_tag = article.find('h2', class_=lambda x: x and 'Card-Title' in x)
            link_tag = article.find('a', class_=lambda x: x and 'Card-Title' in x)
            category_tag = article.find('span', class_=lambda x: x and 'Card-Category' in x)

            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            link = link_tag['href'] if link_tag and link_tag.get('href') else "N/A"
            category = category_tag.get_text(strip=True) if category_tag else "N/A"

            if title != "N/A" and link != "N/A": # Filtra entradas inválidas
                news_items.append({
                    "categoria": category,
                    "titular": title,
                    "enlace": link,
                    "fecha_extraccion": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
    else:
        print(f"Fuente de noticias desconocida: {source_name}")

    return news_items

def process_s3_html_event(event, context):
    """
    Procesa un evento de S3 cuando se crea un archivo HTML.
    Extrae datos y los guarda como CSV en otra ubicación de S3.
    """
    if not S3_BUCKET_NAME:
        print("Error: La variable de entorno 'S3_DATA_BUCKET' no está configurada.")
        return {'statusCode': 500, 'body': 'S3_DATA_BUCKET environment variable not set.'}

    print(f"Bucket de datos principal: {S3_BUCKET_NAME}")

    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        if not key.endswith('.html'):
            print(f"Saltando el archivo no HTML: {key}")
            continue

        print(f"Procesando el archivo S3: {key} del bucket: {bucket_name}")

        try:
            # Descargar el archivo HTML
            response = s3.get_object(Bucket=bucket_name, Key=key)
            html_content = response['Body'].read().decode('utf-8')

            # Extraer el nombre de la fuente del path (ej: eltiempo, elespectador)
            # Asume el formato: headlines/raw/{source_name}/...
            path_parts = key.split('/')
            if len(path_parts) > 2:
                source_name = path_parts[2]
            else:
                source_name = "unknown"
            print(f"Fuente de noticias detectada: {source_name}")

            # Extraer datos de noticias
            news_data = extract_news_data(html_content, source_name)
            
            if not news_data:
                print(f"No se pudieron extraer datos de noticias del archivo: {key}. Posiblemente los selectores son incorrectos o no hay noticias.")
                continue

            # Preparar datos para CSV
            output_buffer = StringIO()
            fieldnames = ["categoria", "titular", "enlace", "fecha_extraccion"]
            writer = csv.DictWriter(output_buffer, fieldnames=fieldnames, delimiter=';') # Usar ; como delimitador
            writer.writeheader()
            writer.writerows(news_data)

            # Determinar el path de salida particionado
            # Espera que el key sea: headlines/raw/{source_name}/{fecha}/{nombre_archivo}.html
            # Ej: headlines/raw/eltiempo/2023-10-26/eltiempo_2023-10-26.html
            # Salida: headlines/final/periodico=eltiempo/year=2023/month=10/day=26/eltiempo_2023-10-26.csv
            
            # Extraer fecha del path
            date_part = key.split('/')[-2] # Asume que la fecha es el penúltimo elemento del path
            if re.match(r'\d{4}-\d{2}-\d{2}', date_part):
                year, month, day = date_part.split('-')
            else:
                # Si la fecha no se puede extraer, usar la fecha actual como fallback
                now = datetime.datetime.now()
                year = now.strftime("%Y")
                month = now.strftime("%m")
                day = now.strftime("%d")
                print(f"Advertencia: No se pudo extraer la fecha del path '{key}'. Usando fecha actual: {year}-{month}-{day}")


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
            # Puedes optar por re-lanzar la excepción o registrar el error para la depuración
            raise e

    return {'statusCode': 200, 'body': 'Procesamiento de eventos S3 completado.'}

# Para pruebas locales
if __name__ == "__main__":
    # Simula un evento S3 para pruebas locales
    mock_s3_event = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {"name": "devlepa-noticias-data"}, # Reemplaza con tu bucket de prueba
                    "object": {"key": "headlines/raw/elespectador/2025-05-25/elespectador_2025-05-25.html"} # Ruta de prueba
                }
            }
        ]
    }
    # Para que esta prueba local funcione, necesitarías:
    # 1. Un archivo HTML en esa ruta S3.
    # 2. Credenciales de AWS configuradas en tu entorno local (o en .env si las descomentas).
    # 3. Que los selectores HTML sean correctos para el contenido del HTML de prueba.
    print("Ejecutando proceso de headlines localmente...")
    response = process_s3_html_event(mock_s3_event, None)
    print(response)