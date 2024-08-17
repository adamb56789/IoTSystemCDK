import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";

const MEASUREMENTS_TABLE_NAME = process.env.MEASUREMENTS_TABLE_NAME;

const docClient = DynamoDBDocumentClient.from(new DynamoDBClient());

export async function handler(event) {
    const { device_id, temperature, humidity, time } = event;

    const params = {
        TableName: MEASUREMENTS_TABLE_NAME,
        Item: {
            device_id,
            time,
            temperature,
            humidity
        }
    };

    console.log("Making request:", params);
    await docClient.send(new PutCommand(params));

    console.log(JSON.stringify({
        _aws: {
            Timestamp: time,
            CloudWatchMetrics: [
                {
                    Namespace: 'IoTDeviceMetrics',
                    Dimensions: [["device_id"]],
                    Metrics: [
                        { Name: "temperature", Unit: "None" },
                        { Name: "humidity", Unit: "Percent" }
                    ]
                }
            ]
        },
        device_id: device_id,
        temperature: temperature,
        humidity: humidity
    }));
}