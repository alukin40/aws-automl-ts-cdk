import { Construct } from "constructs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as cdk from 'aws-cdk-lib';

import { GlueConstruct } from "./glue";
import { TriggerConstruct } from './trigger';
import { LambdaConstruct } from './lambda';
import { SageMakerConstruct } from './create-model';

export interface StateMachineProps {
  resourceBucket: s3.Bucket;
}

export class StateMachine extends Construct {
  public readonly role: iam.Role;

  constructor(scope: Construct, id: string, props: StateMachineProps) {
    super(scope, id);

    const resourceBucket = props.resourceBucket;

    // IAM Role to pass to SageMaker Autopilot
    const sagemakerExecutionRole = new iam.Role(
      this,
      "AutoML-TS-MLOps-Pipeline-SageMaker-Execution-Role",
      {
        assumedBy: new iam.ServicePrincipal("sagemaker.amazonaws.com"),
        roleName: "AutoML-TS-MLOps-Pipeline-Sagemaker-Role",
        managedPolicies: [
          {managedPolicyArn: "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"},
          {managedPolicyArn: "arn:aws:iam::aws:policy/AmazonS3FullAccess"}
        ],
      },
    );
    
    // IAM Role for State Machine
    const stateMachineExecutionRole = new iam.Role(this, 'AutoML-TS-MLOps-Pipeline-StateMachine-Execution-Role', {
        assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
        roleName: 'AutoML-TS-MLOps-Pipeline-Execution-Role',
        managedPolicies: [
            {managedPolicyArn: 'arn:aws:iam::aws:policy/service-role/AWSLambdaRole'},
            {managedPolicyArn: 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'}
        ]
    })

    // Preprocessing
    const preprocess = new GlueConstruct(this, "AutoML-TS-MLOps-Pipeline-Data-Preprocess-Glue", {
      taskName: "AutoML-TS-MLOps-Pipeline-Data-Preprocess-Glue",
      pythonFilePath: "glue/preprocess.py",
      defaultArguments: {
        "--bucket": props.resourceBucket.bucketName,
        "--fileuri": "raw/data.zip",
      },
    });
    
    // Create Autopilot TS Training Job
    const createAutopilotTrainingJob = new LambdaConstruct(this, 'AutoML-TS-MLOps-Pipeline-Create-Autopilot-Job', {
       taskName: 'AutoML-TS-MLOps-Pipeline-Create-Autopilot-Job',
       lambdaCodePath: 'lambda/create-autopilot-job',
       timeout: cdk.Duration.seconds(30),
       environment: {
           SAGEMAKER_ROLE_ARN: sagemakerExecutionRole.roleArn,
           RESOURCE_BUCKET: props.resourceBucket.bucketName
       }
    });
    
    // Check Autopilot Job Status
    const checkJobStatus = new LambdaConstruct(this, 'AutoML-TS-MLOps-Pipeline-Autopilot-Job-Status-Check', {
        taskName: 'AutoML-TS-MLOps-Pipeline-Autopilot-Job-Status-Check',
        lambdaCodePath: 'lambda/check-autopilot-job',
        timeout: cdk.Duration.seconds(30),
        environment: {
            SAGEMAKER_ROLE_ARN: sagemakerExecutionRole.roleArn
        }
    })
    
    // Waiting 5m before checking Autopilot Job Status
    const wait5min = new sfn.Wait(this, 'AutoML-TS-MLOps-Pipeline-Wait5Min', {
        time: sfn.WaitTime.duration(cdk.Duration.minutes(5))
    })
    
    // Finish State Machine if job failed
    const jobFailed = new sfn.Fail(this, 'AutoML-TS-MLOps-Pipeline-Job-Failed', {
      cause: 'Autopilot MLOps Pipeline Job Failed',
      error: 'Autopilot Train Job returned FAILED',
    });
    
    // Temporary, for testing only
    const success = new sfn.Succeed(this, 'We did it!');
    
    // Create a model from the Best trained model from AutoML
    const bestModel = new SageMakerConstruct(this, 'AutoML-TS-MLOps-Pipeline-Best-Model', {
      taskName: 'AutoML-TS-MLOps-Pipeline-Best-Model',
      sourceBucketName: props.resourceBucket.bucketName,
      destinationBucketName: props.resourceBucket.bucketName,
      inputModelName: '$.BestCandidate.InferenceContainer.CandidateName',
      sagemakerRoleArn: sagemakerExecutionRole.roleArn
    });
    
    
    // State Machine Definition
    const definition = preprocess.task
                        .next(createAutopilotTrainingJob.task)
                        .next(wait5min)
                        .next(checkJobStatus.task)
                        .next(new sfn.Choice(this, 'Job Complete?')
                            // Look at the Autopilot Job Status field
                            .when(sfn.Condition.stringEquals('$.AutoMLJobStatus', 'InProgress'), wait5min)
                            .when(sfn.Condition.stringEquals('$.AutoMLJobStatus', 'Completed'), bestModel.createModelTask
                              .next(bestModel.createTransformJob)
                            )
                            .otherwise(jobFailed));
    
    // Creating a State Machine
    const stateMachine = new sfn.StateMachine(this, 'AutoML-TS-MLOps-Pipeline-State-Machine', {
        definition: definition,
        role: stateMachineExecutionRole,
        stateMachineName: 'AutoML-TS-MLOps-Pipeline'
    });
    
    // Train trigger, from S3 Object Create to Lambda, which then initiates State Machine
    const trainTrigger = new TriggerConstruct(this, 'AutoML-TS-MLOps-Pipeline-Train-Trigger', {
        stateMachine: stateMachine,
        resourceBucket: resourceBucket,
        s3Prefix: 'raw/',
        s3Suffix: '.zip'
    });
  }
}
