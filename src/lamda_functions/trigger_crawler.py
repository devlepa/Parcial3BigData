import boto3
import os
from dotenv import load_dotenv

# Carga variables de entorno del archivo .env si existe (solo para desarrollo local)
load_dotenv()

glue = boto3.client('glue')

# Obtiene el nombre del Glue Crawler de las variables de entorno
GLUE_CRAWLER_NAME = os.environ.get('GLUE_CRAWLER_NAME')

def trigger_glue_crawler(event, context):
    """
    Inicia un AWS Glue Crawler.
    """
    if not GLUE_CRAWLER_NAME:
        print("Error: La variable de entorno 'GLUE_CRAWLER_NAME' no está configurada.")
        return {'statusCode': 500, 'body': 'GLUE_CRAWLER_NAME environment variable not set.'}

    print(f"Iniciando Glue Crawler: {GLUE_CRAWLER_NAME}")

    try:
        response = glue.start_crawler(Name=GLUE_CRAWLER_NAME)
        print(f"Crawler {GLUE_CRAWLER_NAME} iniciado exitosamente. Respuesta: {response}")
        return {'statusCode': 200, 'body': f'Crawler {GLUE_CRAWLER_NAME} iniciado exitosamente.'}
    except Exception as e:
        print(f"Error al iniciar el crawler {GLUE_CRAWLER_NAME}: {e}")
        return {'statusCode': 500, 'body': f'Error al iniciar el crawler: {e}'}

# Para pruebas locales
if __name__ == "__main__":
    # Asegúrate de tener el .env configurado con GLUE_CRAWLER_NAME
    response = trigger_glue_crawler(None, None)
    print(response)