import boto3
import csv
from io import StringIO
from urllib.parse import unquote_plus
from bs4 import BeautifulSoup
import re
import os

s3 = boto3.client('s3')

def extract_news_data(html_content, source_newspaper):
    """
    EXTRAE CATEGORÍA, TITULAR Y ENLACE DE LA NOTICIA USANDO BEAUTIFUL SOUP.
    !!! ESTA FUNCIÓN REQUIERE INSPECCIÓN MANUAL DEL HTML DE CADA PERIÓDICO !!!
    Los selectores CSS/etiquetas usados aquí son EJEMPLOS y probablemente
    necesitarán ser ajustados para el HTML actual de El Tiempo y El Espectador.
    """
    soup = BeautifulSoup(html_content, 'lxml') # Usamos lxml para mayor velocidad y robustez
    news_items = []

    if source_newspaper == 'el_tiempo':
        # --- INICIA TU INSPECCIÓN HTML AQUÍ PARA EL TIEMPO ---
        # Busca un patrón común para los artículos. Ej: div con clase 'article-card'
        articles = soup.find_all('article', class_='article-card') # O 'div', 'li', etc.

        if not articles:
            # Intenta con otro patrón si el primero no encuentra nada
            articles = soup.find_all('h2', class_='title') # Puede ser que solo el titular sea el elemento clave

        for article in articles:
            # Ejemplo: Buscar titular dentro del artículo
            headline_tag = article.find(['h2', 'h3'], class_='title') # Busca h2 o h3 con clase 'title'
            link_tag = article.find('a') # Busca el primer enlace dentro del artículo
            category_tag = article.find('p', class_='category') # Busca la categoría explícita

            headline = headline_tag.get_text(strip=True) if headline_tag else 'N/A'
            link = link_tag['href'] if link_tag and 'href' in link_tag.attrs else 'N/A'
            category = category_tag.get_text(strip=True) if category_tag else 'General'

            # Intenta inferir la categoría desde el enlace si no se encontró explícitamente
            if category == 'General' and link and '[eltiempo.com/](https://eltiempo.com/)' in link:
                # Patrón para extraer categoría de la URL: /noticias/CATEGORIA/
                match = re.search(r'eltiempo\.com/(?:noticias|deportes|cultura|economia)/([^/]+)/', link)
                if match:
                    category = match.group(1).replace('-', ' ').title()
                else:
                    # Intenta un patrón más general si el anterior falla
                    match = re.search(r'eltiempo\.com/seccion/([^/]+)/', link)
                    if match:
                        category = match.group(1).replace('-', ' ').title()

            if headline != 'N/A' and link != 'N/A': # Solo añade si los datos esenciales se encontraron
                news_items.append({
                    'category': category,
                    'headline': headline,
                    'link': link
                })
        # --- TERMINA TU INSPECCIÓN HTML AQUÍ PARA EL TIEMPO ---

    elif source_newspaper == 'el_espectador':
        # --- INICIA TU INSPECCIÓN HTML AQUÍ PARA EL ESPECTADOR ---
        articles = soup.find_all('div', class_='Card-Body') # O 'article', 'div' con clase específica

        if not articles:
            articles = soup.find_all('h2', class_='Card-Title') # Otro patrón posible

        for article in articles:
            headline_tag = article.find(['h2', 'h3'], class_='Card-Title')
            link_tag = article.find('a', class_='Card-Link')
            category_tag = article.find('span', class_='Card-Category')

            headline = headline_tag.get_text(strip=True) if headline_tag else 'N/A'
            link = link_tag['href'] if link_tag and 'href' in link_tag.attrs else 'N/A'
            category = category_tag.get_text(strip=True) if category_tag else 'General'

            if category == 'General' and link and '[elespectador.com/](https://elespectador.com/)' in link:
                 match = re.search(r'elespectador\.com/(?:noticias|deportes|cultura|politica|economia)/([^/]+)/', link)
                 if match:
                    category = match.group(1).replace('-', ' ').title()
                 else:
                    match = re.search(r'elespectador\.com/seccion/([^/]+)/', link)
                    if match:
                        category = match.group(1).replace('-', ' ').title()

            if headline != 'N/A' and link != 'N/A':
                news_items.append({
                    'category': category,
                    'headline': headline,
                    'link': link
                })
        # --- TERMINA TU INSPECCIÓN HTML AQUÍ PARA EL ESPECTADOR ---

    return news_items

def process_s3_html_event(event, context):
    """
    Función Lambda activada por eventos de creación de objetos en S3 (prefijo 'headlines/raw/').
    Procesa el HTML, extrae los datos y los guarda como CSV en el prefijo 'headlines/final/'.
    """
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key']) # Decodifica la clave URL-encoded

        print(f"Procesando objeto S3: {key} desde bucket: {bucket_name}")

        try:
            # 1. Descargar el contenido HTML
            obj = s3.get_object(Bucket=bucket_name, Key=key)
            html_content = obj['Body'].read().decode('utf-8')

            # 2. Determinar el periódico y la fecha desde la clave del objeto S3
            # Formato esperado de la clave: headlines/raw/periodico-contenido-yyyy-mm-dd.html
            parts = key.split('/')
            filename = parts[-1] # Ej: el_tiempo-contenido-2023-10-27.html
            name_parts = filename.split('-contenido-')
            if len(name_parts) < 2:
                print(f"Saltando clave mal formada: {key}")
                continue

            newspaper_name = name_parts[0] # Ej: el_tiempo
            date_str = name_parts[1].replace('.html', '') # Ej: 2023-10-27
            year, month, day = date_str.split('-')

            # 3. Extraer datos usando Beautiful Soup
            extracted_data = extract_news_data(html_content, newspaper_name)

            if not extracted_data:
                print(f"No se extrajo información de noticias de {key}. Saltando creación de CSV.")
                continue

            # 4. Preparar contenido CSV en memoria
            csv_buffer = StringIO()
            fieldnames = ['category', 'headline', 'link']
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
            writer.writeheader()
            for item in extracted_data:
                writer.writerow(item)

            csv_content = csv_buffer.getvalue()

            # 5. Definir la clave S3 destino para el CSV (estructura particionada)
            # s3://bucket/headlines/final/periodico=xxx/year=xxx/month=xxx/day=xxx/data.csv
            csv_s3_key = (f'headlines/final/periodico={newspaper_name}/year={year}/'
                          f'month={month}/day={day}/{newspaper_name}_headlines.csv')

            # 6. Subir CSV a S3
            print(f"Subiendo CSV a s3://{bucket_name}/{csv_s3_key}...")
            s3.put_object(Bucket=bucket_name, Key=csv_s3_key, Body=csv_content.encode('utf-8'), ContentType='text/csv')
            print(f"Procesado exitosamente {key} y subido CSV.")

        except Exception as e:
            print(f"Error procesando {key}: {e}")
            # Puedes considerar re-lanzar la excepción o enviar una notificación de error

    return {
        'statusCode': 200,
        'body': 'Procesamiento HTML y carga de CSV completados.'
    }