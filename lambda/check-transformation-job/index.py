import boto3
import os
from json import loads, dumps
from datetime import datetime, timedelta

sagemaker = boto3.client('sagemaker')

def handler(event, context):
    job_name = event['BestCandidate']['CandidateName']
    
    
    full_response = sagemaker.describe_transform_job(
        TransformJobName = job_name
    )
    
    job_status = full_response["TransformJobStatus"] if "TransformJobStatus" in full_response else "N/A"
    
    transform_output = full_response["TransformOutput"] if "TransformOutput" in full_response else "N/A"
        
    # In case Training job will fail.
    failure_reason = full_response["FailureReason"] if "FailureReason" in full_response else "N/A"
    
    # Take only info which we need further.
    return_object = {
        'TransformJobName': job_name,
        'TransformJobStatus': job_status,
        'TransformOutput': transform_output,
        'TransformFailureReason': failure_reason
    }
    
    return return_object