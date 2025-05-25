import boto3
import os
import datetime

# Initialize EMR client
emr_client = boto3.client('emr')

# --- Configuration ---
# IMPORTANT: Adjust these parameters to match your setup and preferences
EMR_CLUSTER_NAME = "NewsMLCluster"
EMR_RELEASE_LABEL = "emr-6.15.0" # Choose a recent, stable EMR release with Spark
EMR_LOG_URI = "s3://your-emr-logs-bucket/emr-logs/" # Dedicated S3 bucket for EMR logs
EC2_SUBNET_ID = "subnet-xxxxxxxxxxxxxxxxx" # Your VPC Subnet ID where EMR should launch
EC2_KEY_PAIR = "your-ec2-key-pair-name" # Your EC2 Key Pair name (for SSH access, optional)
EMR_SERVICE_ROLE = "EMR_DefaultRole" # Your EMR Service Role
EC2_INSTANCE_PROFILE = "EMR_EC2_DefaultRole" # Your EMR EC2 Instance Profile
S3_SCRIPT_LOCATION = "s3://your-glue-scripts-bucket/scripts/run_ml_pipeline.py"
S3_BUCKET_FOR_DATA = "your-unique-bucket-name-here" # Your data S3 bucket

# Instance types and counts
MASTER_INSTANCE_TYPE = "m5.xlarge"
CORE_INSTANCE_TYPE = "m5.xlarge"
NUM_CORE_INSTANCES = 2 # Adjust based on workload

# Bootstrap action (if you need custom libraries like NLTK)
BOOTSTRAP_ACTION_PATH = "s3://your-glue-scripts-bucket/bootstrap/install_pyspark_deps.sh"

def launch_emr_and_run_spark_job(event, context):
    print("Launching EMR cluster and running Spark job...")
    try:
        # Define cluster configuration
        cluster_config = {
            'Name': EMR_CLUSTER_NAME + "-" + datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
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
                'Ec2KeyName': EC2_KEY_PAIR, # Optional
                'KeepJobFlowAliveWhenNoSteps': False, # Important: cluster terminates after job
                'TerminationProtected': False,
                'Ec2SubnetId': EC2_SUBNET_ID,
                # 'EmrManagedMasterSecurityGroup': 'sg-xxxxxxxxxxxxxxxxx', # Optional
                # 'EmrManagedSlaveSecurityGroup': 'sg-xxxxxxxxxxxxxxxxx',  # Optional
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
                    'ActionOnFailure': 'TERMINATE_CLUSTER', # Terminate if job fails
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar', # Generic jar for running commands
                        'Args': [
                            'spark-submit',
                            '--deploy-mode', 'cluster',
                            '--conf', f'spark.pyspark.python={sys.executable}', # Use the Python from EMR
                            S3_SCRIPT_LOCATION,
                            # Optionally pass arguments to your script here
                            # S3_BUCKET_FOR_DATA
                        ]
                    }
                }
            ],
            'BootstrapActions': [ # Optional: Include if you have a bootstrap script
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

        # Launch the EMR cluster
        response = emr_client.run_job_flow(**cluster_config)
        cluster_id = response['JobFlowId']
        print(f"Started EMR cluster with ID: {cluster_id}")

        return {
            'statusCode': 200,
            'body': f'Successfully launched EMR cluster {cluster_id} to run ML pipeline.'
        }

    except Exception as e:
        print(f"Error launching EMR cluster: {e}")
        return {
            'statusCode': 500,
            'body': f'Error launching EMR cluster: {e}'
        }