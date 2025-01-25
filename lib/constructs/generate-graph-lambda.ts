import { Construct } from "constructs";
import { PythonFunction } from '@aws-cdk/aws-lambda-python-alpha';
import { join } from "path";
import { Architecture, FunctionUrlAuthType, Runtime } from "aws-cdk-lib/aws-lambda";
import { Table, TableV2 } from "aws-cdk-lib/aws-dynamodb";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { StringParameter } from "aws-cdk-lib/aws-ssm";
import { CORRECT_PASSWORD_HASH_PARAMETER } from "../iot_system_cdk-stack";
import { CfnOutput, Duration } from "aws-cdk-lib";

export interface GenerateGraphLambdaProps {
    measurementsTable: TableV2;
    locationTable: Table;
    measurementsBucket: Bucket;
}

export class GenerateGraphLambda extends PythonFunction {
    
    constructor(scope: Construct, id: string, props: GenerateGraphLambdaProps) {
        super(scope, id, {
            functionName: "GenerateGraphLambda",
            entry: join(__dirname, '..', 'lambdas'),
            index: "GenerateGraph.py",
            runtime: Runtime.PYTHON_3_13,
            memorySize: 1000,
            timeout: Duration.seconds(10),
            environment: {
                LOCATION_TABLE_NAME: props.locationTable.tableName,
                MEASUREMENTS_TABLE_NAME: props.measurementsTable.tableName,
                BUCKET_NAME: props.measurementsBucket.bucketName,
                PASSWORD_HASH: StringParameter.valueFromLookup(scope, CORRECT_PASSWORD_HASH_PARAMETER)
            },
        });

        props.locationTable.grantReadData(this);
        props.measurementsTable.grantReadData(this);
        props.measurementsBucket.grantRead(this);

        const functionUrl = this.addFunctionUrl({
            authType: FunctionUrlAuthType.NONE,
        });

        new CfnOutput(this, id + "url", {
            value: functionUrl.url,
        });
    }
}