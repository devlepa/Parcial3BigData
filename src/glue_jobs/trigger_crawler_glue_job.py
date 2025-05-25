import boto3
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
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'GLUE_CRAWLER_NAME'])
    GLUE_CRAWLER_NAME = args['GLUE_CRAWLER_NAME']
except Exception as e:
    print(f"Advertencia: No se pudo obtener GLUE_CRAWLER_NAME de los argumentos de Glue Job. Intentando leer de variables de entorno. Error: {e}")
    GLUE_CRAWLER_NAME = os.environ.get('GLUE_CRAWLER_NAME') # Fallback

if not GLUE_CRAWLER_NAME:
    raise ValueError("GLUE_CRAWLER_NAME no está configurado. Por favor, proporiónalo como argumento de Job de Glue o en las variables de entorno.")

glue = boto3.client('glue')

def trigger_glue_crawler_glue():
    """
    Inicia un AWS Glue Crawler. Adaptado para Glue Job.
    """
    print(f"Iniciando Glue Crawler desde Glue Job: {GLUE_CRAWLER_NAME}")

    try:
        response = glue.start_crawler(Name=GLUE_CRAWLER_NAME)
        print(f"Crawler {GLUE_CRAWLER_NAME} iniciado exitosamente. Respuesta: {response}")
    except Exception as e:
        print(f"Error al iniciar el crawler {GLUE_CRAWLER_NAME}: {e}")
        # En un job de Glue, es común relanzar la excepción para que el job marque como fallido
        raise e

if __name__ == '__main__':
    job.init(args['JOB_NAME'], args)
    trigger_glue_crawler_glue()
    job.commit()