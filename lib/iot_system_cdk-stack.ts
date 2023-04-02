import { Stack, StackProps, CfnOutput } from 'aws-cdk-lib';
import { FilterPattern, ILogGroup } from 'aws-cdk-lib/aws-logs'
import { Alarm, ComparisonOperator, Metric, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch'
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Construct } from 'constructs';
import { join } from 'path'
import { SnsAction } from 'aws-cdk-lib/aws-cloudwatch-actions';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { Table, AttributeType, BillingMode } from 'aws-cdk-lib/aws-dynamodb';
import { PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Runtime, FunctionUrlAuthType, LayerVersion, Function } from 'aws-cdk-lib/aws-lambda';
import { AwsCustomResource, PhysicalResourceId, AwsCustomResourcePolicy } from 'aws-cdk-lib/custom-resources';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { PythonFunction } from '@aws-cdk/aws-lambda-python-alpha'
import { CfnDatabase, CfnTable } from 'aws-cdk-lib/aws-timestream';
import { CfnTopicRule } from 'aws-cdk-lib/aws-iot';


const AWS_PANDAS_LAYER_ARN = "arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python37:1"
const MATPLOTLIB_LAYER_ARN = "arn:aws:lambda:eu-west-1:686637384519:layer:Matplotlib_Layer:2"
const CORRECT_PASSWORD_HASH_PARAMETER = '/PicoTherm/CorrectPasswordHash';

export class IotSystemCdkStack extends Stack {

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const snsTopic = new Topic(this, 'IoTSystemErrors')
    this.createLatestMeasurementsResources(snsTopic)
    this.createHistoryGraphResources(snsTopic)
  }

  /**
   * Create all of the resources for getting the latest measurements
   * @param topic SNS topic for receiving notifications
   */
  private createLatestMeasurementsResources(topic: Topic) {
    // Get the URL of the IOT Core broker endpoint
    const IOT_ENDPOINT = this.getIotEndpoint();

    const locationTable = this.createLocationTable();

    const getLatestMeasurementsLambda = this.createLatestMeasurementsLambda(IOT_ENDPOINT, locationTable);

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
    passwordAlarm.addAlarmAction(new SnsAction(topic))
    parameterAlarm.addAlarmAction(new SnsAction(topic))
  }

  /**
   * Create all of the resources for viewing measurement history graphs
   * @param topic SNS topic for receiving notifications
   */
  private createHistoryGraphResources(topic: Topic) {
    const measurementsTable = this.createTimestreamTable()
    const graphLambda = this.createHistoryGraphLambda(measurementsTable)

    this.createIotTimestreamRule(measurementsTable)

    // Alarm on errors and send emails
    const passwordAlarm = this.createOnErrorAlarm(
      graphLambda.logGroup,
      "GenerateHistoryGraphIncorrectPassword",
      "Incorrect password"
    )
    passwordAlarm.addAlarmAction(new SnsAction(topic))
  }

  /**
   * Create a rule which sends picotherm data from IoT to Timestream
   * @param measurementsTable a Timestream CfnTable to insert measurements into
   * @returns a CfnTopicRule that sends data to timestream
   */
  private createIotTimestreamRule(measurementsTable: CfnTable): CfnTopicRule {
    const role = new Role(this, "IotTimestreamAccessRole", {
      assumedBy: new ServicePrincipal("iot.amazonaws.com")
    })

    role.addToPolicy(new PolicyStatement({
      actions: ['timestream:WriteRecords', 'timestream:DescribeEndpoints'],
      resources: [measurementsTable.attrArn]
    }))

    return new CfnTopicRule(this, 'PicothermTimestreamRule', {
      topicRulePayload: {
        actions: [{
          timestream: {
            databaseName: measurementsTable.databaseName,
            dimensions: [{
              name: 'device',
              value: '${topic()}',
            }],
            roleArn: role.roleArn,
            tableName: measurementsTable.attrName
          },
        }],
        sql: 'SELECT temperature, humidity FROM "picotherm/#"',
      }
    });
  }

  /**
   * Creates a Timstream database with a table containing measurements from the devices
   * @returns a measurements Timestream CfnTable
   */
  private createTimestreamTable(): CfnTable {
    const databaseName = "IOT_DB";
    const database = new CfnDatabase(this, "IOT_DB", {
      databaseName: databaseName
    })

    const table = new CfnTable(this, "measurements", {
      databaseName: databaseName,
      tableName: "measurements",
      magneticStoreWriteProperties: { EnableMagneticStoreWrites: false },
      retentionProperties: { MemoryStoreRetentionPeriodInHours: '1', MagneticStoreRetentionPeriodInDays: '21900' }
    })
    table.addDependency(database)
    table.addOverride('DeletionPolicy', 'Retain')
    database.addOverride('DeletionPolicy', 'Retain')

    return table
  }

  /**
   * Python function returning a webpage with a graph displaying historical
   * measurement data from Timestream
   * @returns a lambda function
   */
  private createHistoryGraphLambda(measurementsTable: CfnTable): Function {
    const awsPandasLayer = LayerVersion.fromLayerVersionArn(this, "PandasLayer", AWS_PANDAS_LAYER_ARN);
    const myMatplotlibLayer = LayerVersion.fromLayerVersionArn(this, "MatplotlibLayer", MATPLOTLIB_LAYER_ARN);

    const graphLambda = new PythonFunction(this, 'generateHistoryGraph', {
      entry: join(__dirname, 'lambdas', 'GenerateHistoryGraph'),
      runtime: Runtime.PYTHON_3_7,
      layers: [awsPandasLayer, myMatplotlibLayer],
      environment: {
        CORRECT_PASSWORD_HASH: StringParameter.valueFromLookup(this, CORRECT_PASSWORD_HASH_PARAMETER),
        TIMESTREAM_DB_NAME: measurementsTable.databaseName,
        TIMESTREAM_TABLE_NAME: measurementsTable.attrName
      },
      memorySize: 1000
    });

    // Need access to query Timestream
    graphLambda.addToRolePolicy(new PolicyStatement({
      actions: ['timestream:Select', 'timestream:DescribeEndpoints'],
      resources: ['*']
    }));

    // Generate and output the function URL
    const graphLambdaUrl = graphLambda.addFunctionUrl({
      authType: FunctionUrlAuthType.NONE,
    });
    new CfnOutput(this, 'graphLambdaUrl', {
      value: graphLambdaUrl.url,
    });

    return graphLambda
  }

  /**
   * Table containing mapping between device id and location
   * @returns DynamoDB table
   */
  private createLocationTable(): Table {
    return new Table(this, 'DeviceLocations', {
      partitionKey: {
        name: 'device_id',
        type: AttributeType.STRING
      },
      billingMode: BillingMode.PAY_PER_REQUEST
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
      runtime: Runtime.NODEJS_14_X,
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

  /**
   * Scan the logs with a metric filter, set an alarm to send to SNS if anything
   * is found
   * @param logGroup logs from a lambda function
   * @param metricName name of the metric, also used to construct Alarm name
   * @param errorString the string to search for in the logs for the error
   * @returns an Alarm
   */
  private createOnErrorAlarm(logGroup: ILogGroup, metricName: string, errorString: string): Alarm {
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
      treatMissingData: TreatMissingData.NOT_BREACHING,
      threshold: 0
    })
    return alarm
  }
}
