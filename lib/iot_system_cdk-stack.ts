import { CfnOutput, Stack, StackProps } from 'aws-cdk-lib';
import { Alarm, ComparisonOperator, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import { SnsAction } from 'aws-cdk-lib/aws-cloudwatch-actions';
import { AttributeType, BillingMode, Table, TableV2 } from 'aws-cdk-lib/aws-dynamodb';
import { PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { CfnTopicRule } from 'aws-cdk-lib/aws-iot';
import { Function, FunctionUrlAuthType, Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { AwsCustomResource, AwsCustomResourcePolicy, PhysicalResourceId } from 'aws-cdk-lib/custom-resources';
import { Construct } from 'constructs';
import { join } from 'path';

const CORRECT_PASSWORD_HASH_PARAMETER = '/PicoTherm/CorrectPasswordHash';

export class IotSystemCdkStack extends Stack {

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const measurementsTable = new TableV2(this, 'MeasurementsTable', {
      tableName: 'MeasurementsTable',
      partitionKey: { name: 'device_id', type: AttributeType.STRING },
      sortKey: { name: 'time', type: AttributeType.NUMBER },
      deletionProtection: true
    });

    this.createIotDDBRule(measurementsTable);

    const adminNotificationTopic = new Topic(this, 'IoTSystemErrors')

    const locationTable = new Table(this, 'DeviceLocations', {
      partitionKey: {
        name: 'device_id',
        type: AttributeType.STRING
      },
      billingMode: BillingMode.PAY_PER_REQUEST
    });

    const getLatestMeasurementsLambda = this.createLatestMeasurementsLambda(this.getIotEndpoint(), locationTable);

    const lambdas = [
      getLatestMeasurementsLambda
    ];

    lambdas.forEach(l => {
      const alarmName = `${l}-Errors`;
      const alarm = new Alarm(this, alarmName, {
        metric: l.metricErrors(),
        evaluationPeriods: 1,
        actionsEnabled: true,
        alarmName: alarmName,
        comparisonOperator: ComparisonOperator.GREATER_THAN_THRESHOLD,
        treatMissingData: TreatMissingData.NOT_BREACHING,
        threshold: 0
      });
      alarm.addAlarmAction(new SnsAction(adminNotificationTopic));
    });
  }

  /**
   * Create a rule which sends picotherm data from IoT to Timestream
   * @param measurementsTable a DynamoDB table to insert measurements into
   * @returns a CfnTopicRule that sends data to timestream
   */
  private createIotDDBRule(measurementsTable: TableV2): CfnTopicRule {
    const role = new Role(this, "IotDDBAccessRole", {
      assumedBy: new ServicePrincipal("iot.amazonaws.com")
    });

    measurementsTable.grantWriteData(role);

    return new CfnTopicRule(this, 'PicothermDDBRule', {
      ruleName: 'PicothermDDBRule',
      topicRulePayload: {
        actions: [{
          dynamoDb: {
            tableName: measurementsTable.tableName,
            roleArn: role.roleArn,
            hashKeyField: 'device_id',
            hashKeyValue: '${topic()}',
            hashKeyType: 'STRING',
            rangeKeyField: 'time',
            rangeKeyValue: '${timestamp()}',
            rangeKeyType: 'NUMBER',
            payloadField: 'payload'
          }
        }],
        sql: 'SELECT temperature, humidity FROM "picotherm/#"',
      }
    });
  }

  /**
   * Node.js function returning a webpage with data from a list of devices
   * @param iotEndpoint the URL of the IoT core endpoint
   * @param locationTable a DynamoDB table mapping device ID to location
   * @returns a lambda function
   */
  private createLatestMeasurementsLambda(iotEndpoint: string, locationTable: Table): Function {
    const lambdaFunction = new NodejsFunction(this, 'getLatestMeasurements', {
      entry: join(__dirname, 'lambdas', 'GetLatestMeasurements', 'index.js'),
      bundling: {
        externalModules: [
          'aws-sdk',
        ],
      },
      environment: {
        IOT_ENDPOINT: iotEndpoint,
        TABLE_NAME: locationTable.tableName,
        PASSWORD_HASH: StringParameter.valueFromLookup(this, CORRECT_PASSWORD_HASH_PARAMETER)
      },
      runtime: Runtime.NODEJS_20_X,
      memorySize: 1000
    });

    // Lambda needs perms for location table and IoT
    locationTable.grantReadData(lambdaFunction)
    lambdaFunction.addToRolePolicy(new PolicyStatement({
      actions: ['iot:GetRetainedMessage'],
      resources: ['*']
    }));

    // Generate and output the function URL
    const getLatestMeasurementsUrl = lambdaFunction.addFunctionUrl({
      authType: FunctionUrlAuthType.NONE,
    });
    new CfnOutput(this, 'getLatestMeasurementsUrl', {
      value: getLatestMeasurementsUrl.url,
    });

    return lambdaFunction
  }

  /**
   * Gets the endpoint of the IoT Core broker for accessing the measurements
   * @returns the endpoint URL
   */
  private getIotEndpoint(): string {
    const getIoTEndpoint = new AwsCustomResource(this, 'IoTEndpoint', {
      onCreate: {
        service: 'Iot',
        action: 'describeEndpoint',
        physicalResourceId: PhysicalResourceId.fromResponse('endpointAddress'),
        parameters: {
          "endpointType": "iot:Data-ATS"
        }
      },
      policy: AwsCustomResourcePolicy.fromSdkCalls({ resources: AwsCustomResourcePolicy.ANY_RESOURCE })
    });
    const IOT_ENDPOINT = getIoTEndpoint.getResponseField('endpointAddress')
    return IOT_ENDPOINT
  }
}
