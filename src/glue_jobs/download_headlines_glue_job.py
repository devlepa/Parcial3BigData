import requests
import boto3
import datetime
import os
import sys
from io import StringIO
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

# Obtiene argumentos del Job de Glue. JOB_NAME es estándar.
# S3_DATA_BUCKET debe ser pasado como un argumento de Job en la consola de Glue.
# Si no se pasa como argumento, intenta leer de variables de entorno (para local o Job env vars)
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_DATA_BUCKET'])
    S3_BUCKET_NAME = args['S3_DATA_BUCKET']
except Exception as e:
    print(f"Advertencia: No se pudo obtener S3_DATA_BUCKET de los argumentos de Glue Job. Intentando leer de variables de entorno. Error: {e}")
    S3_BUCKET_NAME = os.environ.get('S3_DATA_BUCKET') # Fallback para pruebas locales o variables de entorno del Job

if not S3_BUCKET_NAME:
    raise ValueError("S3_DATA_BUCKET no está configurado. Por favor, proporciónalo como argumento de Job de Glue o en las variables de entorno.")

s3 = boto3.client('s3')

def download_and_upload_to_s3_glue():
    """
    Descarga titulares de El Tiempo y El Espectador y los sube a S3.
    Adaptado para ser ejecutado como un Glue Job.
    """
    print(f"Iniciando descarga y subida a S3 como Glue Job. Bucket destino: {S3_BUCKET_NAME}")

    current_date = datetime.datetime.now().strftime("%Y-%m-%d")

    news_sources = {
        "eltiempo": "https://www.eltiempo.com/",
        "elespectador": "https://www.elespectador.com/"
    }

    results = []
    for source_name, url in news_sources.items():
        try:
            print(f"Descargando de {source_name} desde {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()

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

    print(f'Descarga y subida de titulares completada: {"; ".join(results)}')

if __name__ == '__main__':
    job.init(args['JOB_NAME'], args) # Inicializa el job con el nombre y los argumentos
    download_and_upload_to_s3_glue()
    job.commit()