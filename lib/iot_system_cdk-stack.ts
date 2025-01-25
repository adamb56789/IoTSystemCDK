import { PythonFunction } from '@aws-cdk/aws-lambda-python-alpha';
import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { Alarm, ComparisonOperator, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import { SnsAction } from 'aws-cdk-lib/aws-cloudwatch-actions';
import { AttributeType, BillingMode, Table, TableV2 } from 'aws-cdk-lib/aws-dynamodb';
import { Rule, RuleTargetInput, Schedule } from 'aws-cdk-lib/aws-events';
import { LambdaFunction } from 'aws-cdk-lib/aws-events-targets';
import { ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { CfnTopicRule } from 'aws-cdk-lib/aws-iot';
import { Architecture, Function, FunctionUrlAuthType, LayerVersion, Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { join } from 'path';
import { GenerateGraphLambda } from './constructs/generate-graph-lambda';

export const CORRECT_PASSWORD_HASH_PARAMETER = '/PicoTherm/CorrectPasswordHash';

export class IotSystemCdkStack extends Stack {

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const measurementsTable = new TableV2(this, 'MeasurementsTable', {
      tableName: 'MeasurementsTable',
      partitionKey: { name: 'device_id', type: AttributeType.STRING },
      sortKey: { name: 'time', type: AttributeType.NUMBER },
      pointInTimeRecovery: true,
      deletionProtection: true,
      removalPolicy: RemovalPolicy.RETAIN
    });

    const storeMeasurementLambda = this.storeMeasurementLambda(measurementsTable);

    this.createIotLambdaRule(storeMeasurementLambda);

    const adminNotificationTopic = new Topic(this, 'IoTSystemErrors')

    const locationTable = new Table(this, 'DeviceLocations', {
      partitionKey: {
        name: 'device_id',
        type: AttributeType.STRING
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.RETAIN,
    });
    
    locationTable.addGlobalSecondaryIndex({
      indexName: 'LocationToId',
      partitionKey: { name: 'location', type: AttributeType.STRING },
    });

    const getLatestMeasurementsLambda = this.createLatestMeasurementsLambda(measurementsTable, locationTable);

    const measurementsBucket = new Bucket(this, 'DataDumpBucket', {
      bucketName: "picotherm-measurement-data",
      removalPolicy: RemovalPolicy.RETAIN
    })

    const dailyS3Lambda = this.aggregateMeasurementsS3(measurementsTable, locationTable, measurementsBucket);

    const generateGraphLambda = new GenerateGraphLambda(this, "GenerateGraphLambda", {measurementsTable, locationTable, measurementsBucket});

    const lambdas = [
      getLatestMeasurementsLambda,
      dailyS3Lambda
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

  private createIotLambdaRule(lambdaFunction: Function): CfnTopicRule {
    const rule = new CfnTopicRule(this, 'PicothermLambdaRule', {
      ruleName: 'PicothermLambdaRule',
      topicRulePayload: {
        sql: 'SELECT topic() as device_id, timestamp() as time, temperature, humidity FROM "picotherm/#"',
        actions: [{
          lambda: {
            functionArn: lambdaFunction.functionArn
          }
        }]
      }
    });

    lambdaFunction.addPermission('IotPermission', {
      principal: new ServicePrincipal('iot.amazonaws.com'),
      sourceArn: rule.attrArn
    });

    return rule
  }

  /**
   * Node.js function returning a webpage with data from a list of devices
   */
  private createLatestMeasurementsLambda(measurementsTable: TableV2, locationTable: Table): Function {
    const lambdaFunction = new NodejsFunction(this, 'getLatestMeasurements', {
      entry: join(__dirname, 'lambdas', 'GetLatestMeasurements', 'index.js'),
      environment: {
        LOCATION_TABLE_NAME: locationTable.tableName,
        MEASUREMENTS_TABLE_NAME: measurementsTable.tableName,
        PASSWORD_HASH: StringParameter.valueFromLookup(this, CORRECT_PASSWORD_HASH_PARAMETER)
      },
      runtime: Runtime.NODEJS_20_X,
      memorySize: 1000,
      reservedConcurrentExecutions: 2 // Prevents denial-of-wallet attack
    });

    measurementsTable.grantReadData(lambdaFunction)
    locationTable.grantReadData(lambdaFunction)

    // Generate and output the function URL
    const getLatestMeasurementsUrl = lambdaFunction.addFunctionUrl({
      authType: FunctionUrlAuthType.NONE,
    });
    new CfnOutput(this, 'getLatestMeasurementsUrl', {
      value: getLatestMeasurementsUrl.url,
    });

    return lambdaFunction
  }

  private storeMeasurementLambda(measurementsTable: TableV2): Function {
    const lambdaFunction = new NodejsFunction(this, 'StoreMeasurement', {
      functionName: 'StoreMeasurement',
      entry: join(__dirname, 'lambdas', 'StoreMeasurement', 'index.js'),
      environment: {
        MEASUREMENTS_TABLE_NAME: measurementsTable.tableName,
      },
      runtime: Runtime.NODEJS_20_X,
      memorySize: 128
    });

    measurementsTable.grantWriteData(lambdaFunction);

    return lambdaFunction;
  }

  private aggregateMeasurementsS3(measurementsTable: TableV2, locationTable: Table, measurementsBucket: Bucket): Function {
    const awsPandasLayer = LayerVersion.fromLayerVersionArn(this, "PandasLayer", "arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python312-Arm64:12");

    const lambda = new PythonFunction(this, 'aggregateMeasurementData', {
      functionName: "AggregateMeasurementData",
      entry: join(__dirname, 'lambdas', 'AggregateMeasurementData'),
      runtime: Runtime.PYTHON_3_13,
      architecture: Architecture.ARM_64,
      layers: [awsPandasLayer],
      environment: {
        MEASUREMENTS_TABLE_NAME: measurementsTable.tableName,
        LOCATION_TABLE_NAME: locationTable.tableName,
        BUCKET_NAME: measurementsBucket.bucketName
      },
      memorySize: 1000,
      timeout: Duration.minutes(15)
    });

    measurementsTable.grantReadData(lambda);
    locationTable.grantReadData(lambda);
    measurementsBucket.grantReadWrite(lambda);

    const dailyRule = new Rule(this, 'DailyS3DumpRule', {
      ruleName: "DailyS3Dump",
      schedule: Schedule.cron({ minute: '0', hour: '3', day: '*', month: '*', year: '*' }),
    });

    dailyRule.addTarget(new LambdaFunction(lambda, {
      event: RuleTargetInput.fromObject({
        frequency: "daily"
      })
    }));

    const monthlyRule = new Rule(this, 'MonthlyS3DumpRule', {
      ruleName: "MonthlyS3Dump",
      schedule: Schedule.cron({ minute: '0', hour: '3', day: '1', month: '*', year: '*' }),
    });

    monthlyRule.addTarget(new LambdaFunction(lambda, {
      event: RuleTargetInput.fromObject({
        frequency: "monthly"
      })
    }));

    const yearlyRule = new Rule(this, 'YearlyS3DumpRule', {
      ruleName: "YearlyS3Dump",
      schedule: Schedule.cron({ minute: '0', hour: '3', day: '1', month: '1', year: '*' }),
    });

    yearlyRule.addTarget(new LambdaFunction(lambda, {
      event: RuleTargetInput.fromObject({
        frequency: "yearly"
      })
    }));

    return lambda;
  }
}
