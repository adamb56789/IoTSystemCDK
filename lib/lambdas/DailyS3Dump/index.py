import json
import os
from datetime import datetime, timedelta

import boto3
import numpy as np


def handler(event, context):

    dynamodb = boto3.client('dynamodb')
    table_name = os.environ['MEASUREMENTS_TABLE_NAME']
    location_table_name = os.environ['LOCATION_TABLE_NAME']

    devices = [item['device_id']['S']
               for item in dynamodb.scan(TableName=location_table_name)['Items']]

    # If there is a data in the inut event, use that instead of today's
    # For backfilling, etc
    input_date = event.get('date')
    if input_date is None:
        end = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        end = datetime.strptime(input_date, '%Y-%m-%d') + timedelta(days=1)

    start = end - timedelta(days=1)
    start_time = int(start.timestamp() * 1000)
    end_time = int(end.timestamp() * 1000)

    for index, device in enumerate(devices):
        response = dynamodb.query(
            TableName=table_name,
            KeyConditionExpression='device_id = :id and #t between :start and :end',
            ExpressionAttributeNames={'#t': 'time'},
            ExpressionAttributeValues={
                ':id': {'S': device},
                ':start': {'N': str(start_time)},
                ':end': {'N': str(end_time)}
            }
        )
        items = response['Items']

        data = [
            [
                float(item['time']['N']),
                float(item['temperature']['N']),
                float(item['humidity']['N'])
            ]
            for item in items
        ]

        array = np.array(data, dtype=np.float64)

        tmp_file_name = f'/tmp/{index}.npy'
        np.save(tmp_file_name, array)

        s3 = boto3.client('s3')
        bucket_name = os.environ['BUCKET_NAME']
        date_str = start.strftime('%Y/%m/%d')

        s3.upload_file(tmp_file_name, bucket_name,
                       f'{device}/{date_str}/data.npy')

    return {
        'statusCode': 200,
        'body': json.dumps('Data successfully processed and uploaded')
    }
