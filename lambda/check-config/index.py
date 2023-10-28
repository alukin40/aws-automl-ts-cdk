import boto3
import os

s3 = boto3.client('s3')

def handler(event, context):

    resource_bucket = os.environ['RESOURCE_BUCKET']

    files_to_check = [
        'config/automl_problem_config.json',
        'config/batch_transform_job_config.json'
    ]

    for file_key in files_to_check:
        try:
            s3.head_object(Bucket=resource_bucket, Key=file_key)
        except:
            return {
                'config_status': 'FAILED',
                'message': f'File {file_key} does not exist.'
            }

    return {
        'config_status': 'SUCCEEDED'
    }