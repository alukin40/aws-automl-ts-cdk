import os
import boto3
from datetime import datetime, timedelta

sagemaker = boto3.client('sagemaker')


def handler(event, context):
    print(event)
    
    sagemaker_role = os.environ['SAGEMAKER_ROLE_ARN']
    resource_bucket = os.environ['RESOURCE_BUCKET']
    
    ts = datetime.now() # note TZ is UTC
    ts = ts.strftime("%Y%m%dT%H%M%S")
    job_name = 'automl-job-'+ts  #os.environ['JOB_NAME']
    
    csv_s3_uri = f's3://{resource_bucket}/input/TTS.csv'
    
    # Define input data config for SageMaker
    input_data_config = [
        {
            'DataSource': {
                'S3DataSource': {
                    'S3DataType': 'S3Prefix',
                    'S3Uri': csv_s3_uri
                }
            }
        }
    ]

    # Define output data config for SageMaker
    output_data_config = {
        'S3OutputPath': f's3://{resource_bucket}/autopilot-output/'
    }
    
    auto_ml_problem_config = {
        'TimeSeriesForecastingJobConfig': {
            'TimeSeriesConfig': {
                'TargetAttributeName': 'Order_Quantity',
                'TimestampAttributeName': 'Timestamp',
                'ItemIdentifierAttributeName': 'Model_ID'
            },
            'ForecastFrequency': '1M',
            'ForecastHorizon': 2,
            # Only for testing doing such limitation, to make it faster.
            #'CompletionCriteria': {
            #    'MaxAutoMLJobRuntimeInSeconds': 600
            #}
        }
    }
    


    # Create the AutoML job
    response = sagemaker.create_auto_ml_job_v2(
        AutoMLJobName=job_name,
        AutoMLJobInputDataConfig=input_data_config,
        OutputDataConfig=output_data_config,
        RoleArn=sagemaker_role,
        AutoMLProblemTypeConfig=auto_ml_problem_config
    )

    return {
        'AutoMLJobResponse': response,
        'AutoMLJobName': job_name
    }