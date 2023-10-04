import { Construct } from "constructs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as lambda_event_sources from "aws-cdk-lib/aws-lambda-event-sources";

export interface TriggerConstructProps {
  stateMachine: sfn.StateMachine;
  resourceBucket: s3.Bucket;
  s3Prefix: string;
  s3Suffix: string;
}

export class TriggerConstruct extends Construct {
  public readonly role: iam.Role;
  public readonly lambda: lambda.Function;
  public readonly task: sfn.TaskStateBase;

  constructor(scope: Construct, id: string, props: TriggerConstructProps) {
    super(scope, id);

    // IAM Role
    this.role = new iam.Role(this, "AutoML-TS-MLOps-Pipeline-Train-Trigger-Role", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      roleName: "AutoML-TS-MLOps-Pipeline-Train-Trigger-Role",
      managedPolicies: [
        {managedPolicyArn: "arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess"},
        {managedPolicyArn: "arn:aws:iam::aws:policy/CloudWatchFullAccess" },
        {managedPolicyArn: "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess" },
        {managedPolicyArn: "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"},
        {managedPolicyArn: "arn:aws:iam::aws:policy/service-role/AWSLambdaRole"},
      ],
    });
    
    // Define Lambda Function for Trigger
    this.lambda = new lambda.Function(this, 'AutoML-TS-MLOps-Pipeline-Upload-Lambda', {
        runtime: lambda.Runtime.PYTHON_3_11,
        role: this.role,
        functionName: 'AutoML-TS-MLOps-Pipeline-Upload-Lambda',
        code: lambda.Code.fromAsset('lambda/trigger'),
        handler: 'index.handler',
        environment: {
            STEP_FUNCTIONS_ARN: props.stateMachine.stateMachineArn
        }
    });
    
    // Add trigger from S3 to Lambda on Object Create
    this.lambda.addEventSource(new lambda_event_sources.S3EventSource(
        props.resourceBucket, {
            events: [
                s3.EventType.OBJECT_CREATED
            ],
            filters: [
                {
                    prefix: props.s3Prefix,
                    suffix: props.s3Suffix
                }
            ]
        }
    ))
  }
}
