import boto3
import os
import datetime
import time

emr_client = boto3.client('emr')

# --- Configuración (se recomienda usar variables de entorno) ---
EMR_CLUSTER_NAME = os.environ.get('EMR_CLUSTER_NAME', 'NewsMLCluster')
EMR_RELEASE_LABEL = os.environ.get('EMR_RELEASE_LABEL', 'emr-6.15.0') # Versión de EMR con Spark
EMR_LOG_URI = os.environ.get('EMR_LOG_URI', 's3://your-emr-logs-bucket/emr-logs/')
EC2_SUBNET_ID = os.environ.get('EC2_SUBNET_ID', 'subnet-xxxxxxxxxxxxxxxxx') # ID de tu subred
EC2_KEY_PAIR = os.environ.get('EC2_KEY_PAIR', 'your-ec2-key-pair-name') # Tu par de llaves EC2 (opcional)
EMR_SERVICE_ROLE = os.environ.get('EMR_SERVICE_ROLE', 'EMR_DefaultRole') # Rol de servicio de EMR
EC2_INSTANCE_PROFILE = os.environ.get('EC2_INSTANCE_PROFILE', 'EMR_EC2_DefaultRole') # Perfil de instancia EC2 para EMR
S3_SCRIPT_LOCATION = os.environ.get('S3_SCRIPT_LOCATION', 's3://your-glue-scripts-bucket/scripts/run_ml_pipeline.py') # Script PySpark
S3_BUCKET_FOR_DATA = os.environ.get('S3_BUCKET', 'your-unique-bucket-name-here') # Bucket de datos principal

# Tipos y número de instancias para el cluster EMR
MASTER_INSTANCE_TYPE = "m5.xlarge"
CORE_INSTANCE_TYPE = "m5.xlarge"
NUM_CORE_INSTANCES = 2 # Ajusta según tu carga de trabajo

# Acción de Bootstrap (para instalar librerías como NLTK, etc.)
BOOTSTRAP_ACTION_PATH = os.environ.get('BOOTSTRAP_ACTION_PATH', 's3://your-glue-scripts-bucket/bootstrap/install_pyspark_deps.sh')


def launch_emr_and_run_spark_job(event, context):
    """
    Función Lambda para lanzar un cluster EMR, ejecutar un script PySpark y apagar el cluster.
    """
    print("Iniciando cluster EMR y ejecutando trabajo Spark...")
    cluster_name = EMR_CLUSTER_NAME + "-" + datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    try:
        # Configuración del cluster
        cluster_config = {
            'Name': cluster_name,
            'ReleaseLabel': EMR_RELEASE_LABEL,
            'Applications': [
                {'Name': 'Spark'}
            ],
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': MASTER_INSTANCE_TYPE,
                        'InstanceCount': 1,
                    },
                    {
                        'Name': "Core nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': CORE_INSTANCE_TYPE,
                        'InstanceCount': NUM_CORE_INSTANCES,
                    }
                ],
                'Ec2KeyName': EC2_KEY_PAIR,
                'KeepJobFlowAliveWhenNoSteps': False, # ¡IMPORTANTE! El cluster se termina después del trabajo
                'TerminationProtected': False,
                'Ec2SubnetId': EC2_SUBNET_ID,
                # 'EmrManagedMasterSecurityGroup': 'sg-xxxxxxxxxxxxxxxxx', # Opcional: para control de red más granular
                # 'EmrManagedSlaveSecurityGroup': 'sg-xxxxxxxxxxxxxxxxx',
            },
            'Configurations': [
                {
                    'Classification': 'spark-defaults',
                    'Properties': {
                        'spark.driver.memory': '2g',
                        'spark.executor.memory': '4g',
                        'spark.executor.cores': '2'
                    }
                }
            ],
            'Steps': [
                {
                    'Name': 'Run PySpark ML Pipeline',
                    'ActionOnFailure': 'TERMINATE_CLUSTER', # Terminar el cluster si el job falla
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar', # Jar genérico para ejecutar comandos
                        'Args': [
                            'spark-submit',
                            '--deploy-mode', 'cluster',
                            '--conf', 'spark.pyspark.python=/usr/bin/python3', # Asegúrate de la ruta de Python en EMR
                            S3_SCRIPT_LOCATION,
                            # Si tu script PySpark necesita argumentos, pásalos aquí:
                            # S3_BUCKET_FOR_DATA
                        ]
                    }
                }
            ],
            'BootstrapActions': [
                {
                    'Name': 'Install PySpark Dependencies',
                    'ScriptBootstrapAction': {
                        'Path': BOOTSTRAP_ACTION_PATH
                    }
                }
            ],
            'LogUri': EMR_LOG_URI,
            'ServiceRole': EMR_SERVICE_ROLE,
            'JobFlowRole': EC2_INSTANCE_PROFILE,
            'VisibleToAllUsers': True,
            'Tags': [
                {'Key': 'Project', 'Value': 'NewsHeadlines'},
                {'Key': 'Environment', 'Value': 'Dev'}
            ]
        }

        # Iniciar el cluster EMR
        response = emr_client.run_job_flow(**cluster_config)
        cluster_id = response['JobFlowId']
        print(f"Cluster EMR iniciado con ID: {cluster_id}")

        # Opcional: Esperar a que el cluster se complete/falle para un feedback más rápido
        # No es necesario para el funcionamiento normal de Lambda, ya que el paso se ejecuta en EMR
        # emr_client.get_waiter('cluster_terminated_without_errors').wait(ClusterId=cluster_id)
        # print(f"Cluster {cluster_id} terminado.")

        return {
            'statusCode': 200,
            'body': f'Lanzado exitosamente el cluster EMR {cluster_id} para ejecutar pipeline ML.'
        }

    except Exception as e:
        print(f"Error al lanzar el cluster EMR: {e}")
        raise e # Re-lanzar la excepción para que Lambda marque un error