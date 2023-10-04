#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { AutopilotMlopsStack } from '../lib/autopilot-mlops-stack';

const app = new cdk.App();
new AutopilotMlopsStack(app, 'AutopilotMlopsStack');
