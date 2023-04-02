import { Stack, StackProps, CfnOutput } from 'aws-cdk-lib';
import { FilterPattern, ILogGroup } from 'aws-cdk-lib/aws-logs'
import { Alarm, ComparisonOperator, Metric, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch'
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Construct } from 'constructs';
import { join } from 'path'
import { SnsAction } from 'aws-cdk-lib/aws-cloudwatch-actions';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { Table, AttributeType } from 'aws-cdk-lib/aws-dynamodb';
import { PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { Runtime, FunctionUrlAuthType } from 'aws-cdk-lib/aws-lambda';
import { AwsCustomResource, PhysicalResourceId, AwsCustomResourcePolicy } from 'aws-cdk-lib/custom-resources';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';


export class IotSystemCdkStack extends Stack {

  /**
   * 
   * @param logGroup logs from a lambda function
   * @param metricName name of the metric, also used to construct Alarm name
   * @param errorString the string to search for in the logs for the error
   * @returns an Alarm
   */
  createOnErrorAlarm(logGroup: ILogGroup, metricName : string, errorString : string) : Alarm {
    const METRIC_NAMESPACE = "IoTSystem"

    logGroup.addMetricFilter(metricName, {
      metricName: metricName,
      metricNamespace: METRIC_NAMESPACE,
      filterPattern: FilterPattern.literal(errorString),
      metricValue: "1"
    })

    const metric = new Metric({
      namespace: METRIC_NAMESPACE,
      metricName: metricName,
      statistic: 'sum'
    })

    const alarm = new Alarm(this, metricName, {
      metric,
      evaluationPeriods: 1,
      actionsEnabled: true,
      alarmName: "IoTSystem " + metricName,
      comparisonOperator: ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: TreatMissingData.IGNORE,
      threshold: 0
    })
    return alarm
  }

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Get the URL of the IOT Core broker endpoint
    const getIoTEndpoint = new AwsCustomResource(this, 'IoTEndpoint', {
      onCreate: {
        service: 'Iot',
        action: 'describeEndpoint',
        physicalResourceId: PhysicalResourceId.fromResponse('endpointAddress'),
        parameters: {
          "endpointType": "iot:Data-ATS"
        }
      },
      policy: AwsCustomResourcePolicy.fromSdkCalls({resources: AwsCustomResourcePolicy.ANY_RESOURCE})
    });
    const IOT_ENDPOINT = getIoTEndpoint.getResponseField('endpointAddress')

    // Table containing mapping between device id and location
    const locationTable = new Table(this, 'DeviceLocations', {
      partitionKey: {
        name: 'device_id',
        type: AttributeType.STRING
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
        PASSWORD_HASH: StringParameter.valueFromLookup(this, '/PicoTherm/CorrectPasswordHash')
      },
      runtime: Runtime.NODEJS_14_X,
      memorySize: 1000
    });

    // Lambda needs perms for location table and IoT
    locationTable.grantReadData(getLatestMeasurementsLambda)
    getLatestMeasurementsLambda.addToRolePolicy(new PolicyStatement({
      actions: ['iot:GetRetainedMessage'],
      resources: ['*']
    }));

    // Generate and output the function URL
    const getLatestMeasurementsUrl = getLatestMeasurementsLambda.addFunctionUrl({
      authType: FunctionUrlAuthType.NONE,
    });
    new CfnOutput(this, 'getLatestMeasurementsUrl', {
      value: getLatestMeasurementsUrl.url,
    });

    // Alarm on errors and send emails
    const passwordAlarm = this.createOnErrorAlarm(
      getLatestMeasurementsLambda.logGroup,
      "GetLatestMeasurementsIncorrectPassword",
      "Incorrect password"
    )
    const parameterAlarm = this.createOnErrorAlarm(
      getLatestMeasurementsLambda.logGroup,
      "GetLatestMeasurementsIncorrectParameters",
      "Incorrect parameters"
    )

    const topic = new Topic(this, 'IoTSystemErrors');

    passwordAlarm.addAlarmAction(new SnsAction(topic))
    parameterAlarm.addAlarmAction(new SnsAction(topic))
  }
}
