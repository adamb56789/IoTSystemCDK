import { Stack, StackProps, CfnOutput, Aws } from 'aws-cdk-lib';
import {
  aws_iam as iam,
  aws_dynamodb as db,
  aws_lambda as lambda,
  custom_resources as customResource,
  aws_ssm as ssm
} from 'aws-cdk-lib';
import { NodejsFunction, NodejsFunctionProps } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Construct } from 'constructs';
import { join } from 'path'


export class IotSystemCdkStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Get the URL of the IOT Core broker endpoint
    const getIoTEndpoint = new customResource.AwsCustomResource(this, 'IoTEndpoint', {
      onCreate: {
        service: 'Iot',
        action: 'describeEndpoint',
        physicalResourceId: customResource.PhysicalResourceId.fromResponse('endpointAddress'),
        parameters: {
          "endpointType": "iot:Data-ATS"
        }
      },
      policy: customResource.AwsCustomResourcePolicy.fromSdkCalls({resources: customResource.AwsCustomResourcePolicy.ANY_RESOURCE})
    });
    const IOT_ENDPOINT = getIoTEndpoint.getResponseField('endpointAddress')

    // Table containing mapping between device id and location
    const locationTable = new db.Table(this, 'DeviceLocations', {
      partitionKey: {
        name: 'device_id',
        type: db.AttributeType.STRING
      }
    });

    // Allow it to scale all the way down to 1 because expecting low usage
    locationTable.autoScaleWriteCapacity({
      minCapacity: 1,
      maxCapacity: 5,
    }).scaleOnUtilization({ targetUtilizationPercent: 75 });
    locationTable.autoScaleReadCapacity({
      minCapacity: 1,
      maxCapacity: 5,
    }).scaleOnUtilization({ targetUtilizationPercent: 75 });

    // Node lambda returning webpage with data from a list of devices
    const getLatestMeasurementsLambda = new NodejsFunction(this, 'getLatestMeasurements', {
      entry: join(__dirname, 'lambdas', 'GetLatestMeasurements', 'index.js'),
      bundling: {
        externalModules: [
          'aws-sdk',
        ],
      },
      environment: {
        IOT_ENDPOINT: IOT_ENDPOINT,
        TABLE_NAME: locationTable.tableName,
        PASSWORD_HASH: ssm.StringParameter.valueFromLookup(this, '/PicoTherm/CorrectPasswordHash')
      },
      runtime: lambda.Runtime.NODEJS_14_X,
      memorySize: 1000
    });

    // Lambda needs perms for location table and IoT
    locationTable.grantReadData(getLatestMeasurementsLambda)
    getLatestMeasurementsLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['iot:GetRetainedMessage'],
      resources: ['*']
    }));

    const getLatestMeasurementsUrl = getLatestMeasurementsLambda.addFunctionUrl({
      authType: lambda.FunctionUrlAuthType.NONE,
    });
    new CfnOutput(this, 'getLatestMeasurementsUrl', {
      value: getLatestMeasurementsUrl.url,
    });
  }
}
