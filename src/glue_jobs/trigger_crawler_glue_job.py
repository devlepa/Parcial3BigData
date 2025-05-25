import boto3
import sys

from awsglue.utils import getResolvedOptions

glue = boto3.client('glue')

# Obtener el nombre del crawler de los argumentos del job de Glue
args = getResolvedOptions(sys.argv, ['GLUE_CRAWLER_NAME'])
GLUE_CRAWLER_NAME = args['GLUE_CRAWLER_NAME']

print(f"Iniciando Job de Glue: Activar Crawler para {GLUE_CRAWLER_NAME}")

def trigger_glue_crawler_glue():
    """
    Función principal para el Job de Glue que activa un Glue Crawler.
    """
    print(f"Intentando iniciar el crawler de Glue: {GLUE_CRAWLER_NAME}")
    try:
        response = glue.start_crawler(Name=GLUE_CRAWLER_NAME)
        print(f"Crawler de Glue iniciado exitosamente: {GLUE_CRAWLER_NAME}. Respuesta: {response}")
    except glue.exceptions.CrawlerRunningException:
        print(f"El Crawler {GLUE_CRAWLER_NAME} ya está en ejecución. Saltando.")
    except Exception as e:
        print(f"Error al iniciar el crawler de Glue {GLUE_CRAWLER_NAME}: {e}")
        raise # Re-lanzar la excepción para que el job de Glue falle si el crawler no puede iniciarse

    print("Proceso de activación del Crawler de Glue completado para Job de Glue 3.")

if __name__ == '__main__':
    trigger_glue_crawler_glue()