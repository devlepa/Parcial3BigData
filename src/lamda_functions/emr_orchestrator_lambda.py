import boto3
import os
import datetime
import time
from dotenv import load_dotenv

# Carga variables de entorno del archivo .env si existe (solo para desarrollo local)
load_dotenv()

emr_client = boto3.client('emr')

# Obtiene todas las variables de entorno necesarias
EMR_CLUSTER_NAME = os.environ.get('EMR_CLUSTER_NAME')
EMR_RELEASE_LABEL = os.environ.get('EMR_RELEASE_LABEL')
S3_EMR_LOG_URI = os.environ.get('S3_EMR_LOGS_BUCKET')
EC2_SUBNET_ID = os.environ.get('EC2_SUBNET_ID')
EC2_KEY_PAIR = os.environ.get('EC2_KEY_PAIR') # Opcional, pero incluido para la plantilla
EMR_SERVICE_ROLE = os.environ.get('EMR_SERVICE_ROLE') # Rol de servicio EMR (ej: EMR_DefaultRole)
EC2_INSTANCE_PROFILE = os.environ.get('EC2_INSTANCE_PROFILE') # Perfil de instancia EC2 para EMR (ej: EMR_EC2_DefaultRole)
S3_SCRIPT_LOCATION = os.environ.get('S3_SCRIPT_LOCATION') # Ruta del script PySpark en S3
S3_DATA_BUCKET_FOR_SPARK = os.environ.get('S3_DATA_BUCKET') # Bucket principal de datos para que Spark lo lea
BOOTSTRAP_ACTION_PATH = os.environ.get('BOOTSTRAP_ACTION_PATH') # Ruta del script de bootstrap en S3

# Configuración de instancias EMR (puedes ajustar estos valores)
MASTER_INSTANCE_TYPE = "m5.xlarge"
CORE_INSTANCE_TYPE = "m5.xlarge"
NUM_CORE_INSTANCES = 2 # Cantidad de nodos core (excluyendo el master)

def launch_emr_and_run_spark_job(event, context):
    """
    Lanza un cluster EMR transitorio y ejecuta un job PySpark.
    """
    print("Iniciando lanzamiento de cluster EMR y ejecución de job Spark.")

    # Validar que todas las variables de entorno críticas estén seteadas
    required_vars = {
        'EMR_CLUSTER_NAME': EMR_CLUSTER_NAME,
        'EMR_RELEASE_LABEL': EMR_RELEASE_LABEL,
        'S3_EMR_LOGS_BUCKET': S3_EMR_LOG_URI,
        'EC2_SUBNET_ID': EC2_SUBNET_ID,
        'EMR_SERVICE_ROLE': EMR_SERVICE_ROLE,
        'EC2_INSTANCE_PROFILE': EC2_INSTANCE_PROFILE,
        'S3_SCRIPT_LOCATION': S3_SCRIPT_LOCATION,
        'S3_DATA_BUCKET': S3_DATA_BUCKET_FOR_SPARK,
        'BOOTSTRAP_ACTION_PATH': BOOTSTRAP_ACTION_PATH # Bootstrap es opcional, pero si está en .env, validarlo
    }

    missing_vars = [name for name, value in required_vars.items() if value is None]
    if missing_vars:
        print(f"Error: Faltan variables de entorno críticas: {', '.join(missing_vars)}")
        return {'statusCode': 500, 'body': f'Faltan variables de entorno críticas: {", ".join(missing_vars)}'}
    
    # Asegúrate de que el S3_EMR_LOG_URI tenga el formato correcto para EMR
    formatted_log_uri = f"s3://{S3_EMR_LOG_URI}/emr-logs/"

    # Configuración de bootstrap action (instalar dependencias de PySpark si es necesario)
    bootstrap_actions = []
    if BOOTSTRAP_ACTION_PATH:
        # Asegúrate de que BOOTSTRAP_ACTION_PATH sea un URI s3 válido, no solo el bucket
        if not BOOTSTRAP_ACTION_PATH.startswith('s3://'):
             print(f"Advertencia: BOOTSTRAP_ACTION_PATH '{BOOTSTRAP_ACTION_PATH}' no parece ser un URI S3 válido. Asegúrate de que tenga el prefijo 's3://'.")
        
        bootstrap_actions.append({
            'Name': 'Install PySpark Dependencies',
            'ScriptBootstrapAction': {
                'Path': BOOTSTRAP_ACTION_PATH
            }
        })

    try:
        response = emr_client.run_job_flow(
            Name=EMR_CLUSTER_NAME,
            ReleaseLabel=EMR_RELEASE_LABEL,
            Instances={
                'MasterInstanceType': MASTER_INSTANCE_TYPE,
                'SlaveInstanceType': CORE_INSTANCE_TYPE,
                'InstanceCount': NUM_CORE_INSTANCES + 1,  # Un master + N core instances
                'KeepJobFlowAliveWhenNoSteps': False, # Termina el cluster cuando el job finaliza
                'TerminationProtected': False, # No proteger el cluster de la terminación manual
                'Ec2SubnetId': EC2_SUBNET_ID,
                'Ec2KeyName': EC2_KEY_PAIR if EC2_KEY_PAIR else None # Opcional: solo si usas un key pair
            },
            Applications=[{'Name': 'Spark'}], # Asegúrate de que Spark esté instalado
            VisibleToAllUsers=True,
            JobFlowRole=EC2_INSTANCE_PROFILE, # Rol que las instancias EC2 de EMR asumen
            ServiceRole=EMR_SERVICE_ROLE, # Rol de servicio para el cluster EMR
            LogUri=formatted_log_uri,
            Steps=[
                {
                    'Name': 'Run Spark ML Pipeline',
                    'ActionOnFailure': 'TERMINATE_CLUSTER', # Terminar el cluster si el job Spark falla
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar', # Jar genérico para ejecutar comandos
                        'Args': [
                            'spark-submit',
                            # Pasa el script PySpark
                            S3_SCRIPT_LOCATION,
                            # Pasa variables adicionales como argumentos al script PySpark si es necesario
                            '--s3_data_bucket', S3_DATA_BUCKET_FOR_SPARK
                        ]
                    }
                }
            ],
            BootstrapActions=bootstrap_actions,
            Configurations=[ # Configuración adicional para el cluster Spark
                {
                    'Classification': 'spark-env',
                    'Configurations': [
                        {
                            'Classification': 'export',
                            'Properties': {
                                # Pasa variables de entorno al entorno de Spark (accesibles via os.environ.get en PySpark)
                                'S3_DATA_BUCKET': S3_DATA_BUCKET_FOR_SPARK,
                                'S3_GLUE_SCRIPTS_BUCKET': os.environ.get('S3_GLUE_SCRIPTS_BUCKET', ''), # Asegura que no sea None
                                'S3_EMR_LOGS_BUCKET': os.environ.get('S3_EMR_LOGS_BUCKET', '') # Asegura que no sea None
                            }
                        }
                    ]
                }
            ]
        )
        cluster_id = response['JobFlowId']
        print(f"Cluster EMR lanzado con ID: {cluster_id}")
        return {'statusCode': 200, 'body': f'Cluster EMR {cluster_id} lanzado exitosamente.'}

    except Exception as e:
        print(f"Error al lanzar el cluster EMR: {e}")
        return {'statusCode': 500, 'body': f'Error al lanzar el cluster EMR: {e}'}

# Para pruebas locales
if __name__ == "__main__":
    # Asegúrate de tener el .env configurado con todas las variables de EMR
    response = launch_emr_and_run_spark_job(None, None)
    print(response)