import boto3
import os

# Initialize Glue client
glue = boto3.client('glue')

# Get the crawler name from environment variables set in zappa_settings.json
GLUE_CRAWLER_NAME = os.environ.get('GLUE_CRAWLER_NAME', 'headlines_csv_crawler')

def trigger_glue_crawler(event, context):
    """
    Función Lambda para activar un crawler de AWS Glue.
    Puede ser invocada manualmente o mediante un evento programado.
    """
    print(f"Intentando iniciar el crawler de Glue: {GLUE_CRAWLER_NAME}")
    try:
        response = glue.start_crawler(Name=GLUE_CRAWLER_NAME)
        print(f"Crawler de Glue iniciado exitosamente: {GLUE_CRAWLER_NAME}. Respuesta: {response}")
        return {
            'statusCode': 200,
            'body': f'Crawler de Glue {GLUE_CRAWLER_NAME} iniciado exitosamente.'
        }
    except glue.exceptions.CrawlerRunningException:
        print(f"El Crawler {GLUE_CRAWLER_NAME} ya está en ejecución. Saltando.")
        return {
            'statusCode': 200,
            'body': f'El Crawler {GLUE_CRAWLER_NAME} ya está en ejecución.'
        }
    except Exception as e:
        print(f"Error al iniciar el crawler de Glue {GLUE_CRAWLER_NAME}: {e}")
        # Considera re-lanzar la excepción para que Lambda marque un error
        raise e