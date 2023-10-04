import { Duration, Stack, StackProps, RemovalPolicy } from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";

import { Construct } from "constructs";
import { StateMachine } from "./construct/state-machine";

export class AutopilotMlopsStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const resourceBucket = new s3.Bucket(
      this,
      "AutoML-TS-MLOps-Pipeline-Bucket",
      {
        bucketName: `automl-ts-mlops-pipeline-resource-bucket-${
          Stack.of(this).account
        }`,
        versioned: false,
        autoDeleteObjects: true,
        removalPolicy: RemovalPolicy.DESTROY,
      },
    );

    const stateMachine = new StateMachine(
      this,
      "AutoML-TS-MLOps-Pipeline-StateMachine",
      {
        resourceBucket: resourceBucket,
      },
    );
  }
}
