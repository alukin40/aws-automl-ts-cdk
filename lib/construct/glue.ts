import {Construct} from 'constructs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as sfn_tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';

import * as glue from "@aws-cdk/aws-glue-alpha";

export interface GlueConstructProps {
    taskName: string,
    pythonFilePath: string,
    defaultArguments?: {
        [key:string]: string;
    },
    arguments?: {
        [key:string]: any;
    }
}

export class GlueConstruct extends Construct {
    public readonly role: iam.Role;
    public readonly task: sfn_tasks.GlueStartJobRun;
    
    constructor(scope: Construct, id: string, props: GlueConstructProps) {
        super(scope, id);
        
        // IAM Role
        this.role = new iam.Role(this, `${props.taskName}-Role`, {
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            roleName: `${props.taskName}-Role`,
            managedPolicies: [
                {managedPolicyArn: 'arn:aws:iam::aws:policy/AmazonS3FullAccess'},
                {managedPolicyArn: 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'},
            ]
        });
        
        // Glue Python Job
        const pythonJob = new glue.Job(this, `${props.taskName}-Python-Job`, {
            executable: glue.JobExecutable.pythonShell({
               glueVersion: glue.GlueVersion.V3_0,
               pythonVersion: glue.PythonVersion.THREE_NINE,
               script: glue.Code.fromAsset(props.pythonFilePath)
            }),
            role: this.role,
            jobName: props.taskName,
            //workerType: glue.WorkerType.G_1X,
            //workerCount: 1,
            defaultArguments: props.defaultArguments
        });
        
        // StepFunction Task
        this.task = new sfn_tasks.GlueStartJobRun(this, `${props.taskName}-Task-Job`, {
            glueJobName: pythonJob.jobName,
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            resultPath: sfn.JsonPath.stringAt('$.result'),
            arguments: sfn.TaskInput.fromObject(props.arguments!)
        });
    }
    
}
