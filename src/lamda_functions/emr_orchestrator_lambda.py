# src/lambda_functions/emr_orchestrator_lambda.py
import os
import boto3
import json

emr_client = boto3.client('emr')

def launch_emr_and_run_spark_job(event, context):
    print("Starting launch_emr_and_run_spark_job Lambda function.")

    s3_data_bucket = os.environ.get('S3_DATA_BUCKET')
    s3_glue_scripts_bucket = os.environ.get('S3_GLUE_SCRIPTS_BUCKET')
    emr_cluster_name = os.environ.get('EMR_CLUSTER_NAME')
    emr_release_label = os.environ.get('EMR_RELEASE_LABEL')
    ec2_subnet_id = os.environ.get('EC2_SUBNET_ID')
    ec2_key_pair = os.environ.get('EC2_KEY_PAIR')
    emr_service_role = os.environ.get('EMR_SERVICE_ROLE')
    ec2_instance_profile = os.environ.get('EC2_INSTANCE_PROFILE')
    s3_script_location = os.environ.get('S3_SCRIPT_LOCATION')
    bootstrap_action_path = os.environ.get('BOOTSTRAP_ACTION_PATH')
    s3_emr_logs_bucket = os.environ.get('S3_EMR_LOGS_BUCKET')

    required_env_vars = [
        'S3_DATA_BUCKET', 'S3_GLUE_SCRIPTS_BUCKET', 'EMR_CLUSTER_NAME', 'EMR_RELEASE_LABEL',
        'EC2_SUBNET_ID', 'EMR_SERVICE_ROLE', 'EC2_INSTANCE_PROFILE',
        'S3_SCRIPT_LOCATION', 'BOOTSTRAP_ACTION_PATH', 'S3_EMR_LOGS_BUCKET'
    ]
    for var in required_env_vars:
        if not os.environ.get(var):
            print(f"ERROR: Environment variable {var} not set.")
            return {'statusCode': 500, 'body': f'Missing env var: {var}'}

    emr_config = {
        'Name': emr_cluster_name,
        'ReleaseLabel': emr_release_label,
        'Instances': {
            'MasterInstanceFleet': {
                'Name': 'MasterFleet',
                'InstanceType': 'm5.xlarge',
                'TargetOnDemandCapacity': 1,
                'EbsConfiguration': {
                    'EbsBlockDevices': [{'VolumeSizeInGB': 64, 'VolumeType': 'gp2'}]
                },
            },
            'CoreInstanceFleet': {
                'Name': 'CoreFleet',
                'InstanceType': 'm5.xlarge',
                'TargetOnDemandCapacity': 1,
                'EbsConfiguration': {
                    'EbsBlockDevices': [{'VolumeSizeInGB': 64, 'VolumeType': 'gp2'}]
                },
            },
            'Ec2SubnetId': ec2_subnet_id,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        'Applications': [{'Name': 'Spark'}],
        'JobFlowRole': ec2_instance_profile,
        'ServiceRole': emr_service_role,
        'LogUri': f's3://{s3_emr_logs_bucket}/emr-logs/',
        'BootstrapActions': [
            {
                'Name': 'Install Python Dependencies',
                'ScriptBootstrapAction': {
                    'Path': bootstrap_action_path,
                }
            },
        ],
        'Steps': [
            {
                'Name': 'Run Spark ML Job',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        '--conf', 'spark.yarn.submit.waitAppCompletion=true',
                        s3_script_location,
                        '--JOB_NAME', 'NewsHeadlinesMLJob',
                        '--input_bucket', s3_data_bucket,
                        '--output_path', f's3://{s3_data_bucket}/model_output/'
                    ]
                }
            }
        ],
        'VisibleToAllUsers': True,
        'ManagedScalingPolicy': {
            'ComputeLimits': {
                'UnitType': 'Instances',
                'MinimumCapacityUnits': 1,
                'MaximumCapacityUnits': 5,
                'MaximumOnDemandCapacityUnits': 5,
            }
        }
    }

    if ec2_key_pair and ec2_key_pair.strip():
        emr_config['Instances']['Ec2KeyName'] = ec2_key_pair

    try:
        print(f"Launching EMR cluster: {emr_cluster_name}")
        response = emr_client.run_job_flow(**emr_config)
        cluster_id = response['JobFlowId']
        print(f"EMR cluster {cluster_id} launched successfully.")
        return {
            'statusCode': 200,
            'body': json.dumps(f'EMR cluster {cluster_id} launched.')
        }
    except Exception as e:
        print(f"Error launching EMR cluster: {e}")
        import traceback
        traceback.print_exc()
        raise e