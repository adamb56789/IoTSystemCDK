import json
from datetime import datetime, timedelta

import boto3

lambda_client = boto3.client('lambda')

start_date = datetime.strptime('2022-11-22', '%Y-%m-%d')
end_date = datetime.strptime('2024-08-22', '%Y-%m-%d')

while start_date <= end_date:
    event = {
        'date': start_date.strftime('%Y-%m-%d'),
    }

    response = lambda_client.invoke(
        FunctionName='DailyS3Dump',
        InvocationType='Event',
        Payload=json.dumps(event)
    )

    start_date += timedelta(days=1)

print("Backfilling triggered for all dates.")
