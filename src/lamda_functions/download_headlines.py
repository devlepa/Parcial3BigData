import requests
import boto3
import datetime
import os
from dotenv import load_dotenv

# Carga variables de entorno del archivo .env si existe (solo para desarrollo local)
load_dotenv()

s3 = boto3.client('s3')

# Obtiene el nombre del bucket S3 de las variables de entorno
S3_BUCKET_NAME = os.environ.get('S3_DATA_BUCKET')

def download_and_upload_to_s3(event, context):
    """
    Descarga titulares de El Tiempo y El Espectador y los sube a S3.
    """
    if not S3_BUCKET_NAME:
        print("Error: La variable de entorno 'S3_DATA_BUCKET' no está configurada.")
        return {'statusCode': 500, 'body': 'S3_DATA_BUCKET environment variable not set.'}

    print(f"Iniciando descarga y subida a S3. Bucket destino: {S3_BUCKET_NAME}")

    current_date = datetime.datetime.now().strftime("%Y-%m-%d")

    # URL y prefijos de archivos
    news_sources = {
        "eltiempo": "https://www.eltiempo.com/",
        "elespectador": "https://www.elespectador.com/"
    }

    results = []
    for source_name, url in news_sources.items():
        try:
            print(f"Descargando de {source_name} desde {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status() # Lanza un error para códigos de estado HTTP erróneos

            html_content = response.text
            file_key = f"headlines/raw/{source_name}/{current_date}/{source_name}_{current_date}.html"

            print(f"Subiendo {source_name} HTML a s3://{S3_BUCKET_NAME}/{file_key}")
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=file_key, Body=html_content)
            results.append(f"Successfully uploaded {source_name} to {file_key}")

        except requests.exceptions.RequestException as e:
            error_message = f"Error al descargar de {source_name}: {e}"
            print(error_message)
            results.append(error_message)
        except Exception as e:
            error_message = f"Error inesperado al procesar {source_name}: {e}"
            print(error_message)
            results.append(error_message)

    return {
        'statusCode': 200,
        'body': f'Descarga y subida de titulares completada: {"; ".join(results)}'
    }

# Para pruebas locales o si se ejecuta directamente
if __name__ == "__main__":
    # Asegúrate de tener el .env configurado con S3_DATA_BUCKET
    response = download_and_upload_to_s3(None, None)
    print(response)