import requests
import boto3
import datetime
import os
import sys

# Para los jobs de Glue, los argumentos se pasan via sys.argv o Glue's getResolvedOptions
from awsglue.utils import getResolvedOptions

s3 = boto3.client('s3')

# Obtener parámetros pasados al job de Glue
# Asegúrate de que 'S3_BUCKET' se pase como un argumento de job en la configuración de Glue.
args = getResolvedOptions(sys.argv, ['S3_BUCKET'])
S3_BUCKET_NAME = args['S3_BUCKET']

print(f"Iniciando Job de Glue: Descargar Titulares a bucket S3: {S3_BUCKET_NAME}")

def download_and_upload_to_s3_glue():
    """
    Función principal para el Job de Glue que descarga HTML y lo sube a S3.
    """
    today = datetime.date.today()
    date_str = today.strftime('%Y-%m-%d')

    newspapers = {
        'el_tiempo': '[https://www.eltiempo.com/](https://www.eltiempo.com/)',
        'el_espectador': '[https://www.elespectador.com/](https://www.elespectador.com/)',
        # 'publimetro': '[https://www.publimetro.co/](https://www.publimetro.co/)',
    }

    for name, url in newspapers.items():
        try:
            print(f"Intentando descargar {url}...")
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            content = response.text

            s3_key = f'headlines/raw/{name}-contenido-{date_str}.html'

            print(f"Subiendo contenido de {name} a s3://{S3_BUCKET_NAME}/{s3_key}...")
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=content.encode('utf-8'), ContentType='text/html')
            print(f"Contenido de {name} subido exitosamente.")

        except requests.exceptions.RequestException as e:
            print(f"Error descargando {url}: {e}")
        except boto3.exceptions.S3UploadFailedError as e:
            print(f"Error subiendo a S3 para {name}: {e}")
        except Exception as e:
            print(f"Un error inesperado ocurrió para {name}: {e}")

    print("Proceso de descarga y carga completado para Job de Glue 1.")

if __name__ == '__main__':
    download_and_upload_to_s3_glue()