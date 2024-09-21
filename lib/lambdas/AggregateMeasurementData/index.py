import json
import os
from datetime import datetime, timedelta, date
import calendar
import boto3
import numpy as np

def handler(event, context):
    print(event)
    frequency = event.get('frequency', 'daily').lower()
    input_date = event.get('date')

    dynamodb = boto3.client('dynamodb')
    s3 = boto3.client('s3')
    table_name = os.environ['MEASUREMENTS_TABLE_NAME']
    location_table_name = os.environ['LOCATION_TABLE_NAME']
    bucket_name = os.environ['BUCKET_NAME']

    devices = [item['device_id']['S']
               for item in dynamodb.scan(TableName=location_table_name)['Items']]

    if frequency == 'daily':
        process_daily(devices, dynamodb, s3, table_name, bucket_name, input_date)
    elif frequency == 'monthly':
        process_monthly(devices, s3, bucket_name, input_date)
    elif frequency == 'yearly':
        process_yearly(devices, s3, bucket_name, input_date)
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid frequency parameter.')
        }

    return {
        'statusCode': 200,
        'body': json.dumps(f'Data successfully processed and uploaded for {frequency}.')
    }

def process_daily(devices, dynamodb, s3, table_name, bucket_name, input_date):
    if input_date is None:
        end = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        end = datetime.strptime(input_date, '%Y-%m-%d') + timedelta(days=1)
    start = end - timedelta(days=1)

    start_time = int(start.timestamp() * 1000)
    end_time = int(end.timestamp() * 1000)

    date_str = start.strftime('%Y/%m/%d')

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
        print(f"Writing {array.shape}")
        np.save(tmp_file_name, array)
        s3_key = f'{device}/{date_str}/data.npy'
        s3.upload_file(tmp_file_name, bucket_name, s3_key)

def process_monthly(devices, s3, bucket_name, input_date):
    if input_date is None:
        today = date.today()
        first_day_this_month = date(today.year, today.month, 1)
        last_month_end = first_day_this_month - timedelta(days=1)
        start = date(last_month_end.year, last_month_end.month, 1)
        end = date(last_month_end.year, last_month_end.month, last_month_end.day)
    else:
        specified_date = datetime.strptime(input_date, '%Y-%m-%d').date()
        start = date(specified_date.year, specified_date.month, 1)
        end = date(specified_date.year, specified_date.month, calendar.monthrange(start.year, start.month)[1])

    date_str = start.strftime('%Y/%m')

    for device in devices:
        monthly_data = []

        day = start
        while day <= end:
            day_str = day.strftime('%Y/%m/%d')
            s3_key = f'{device}/{day_str}/data.npy'

            try:
                tmp_file_name = f'/tmp/{device.replace("/", "_")}_{day.strftime("%Y%m%d")}.npy'
                s3.download_file(bucket_name, s3_key, tmp_file_name)
                daily_array = np.load(tmp_file_name)
                print(f"Loaded {s3_key} containing {daily_array.shape}")
                if daily_array.shape[0] != 0 and daily_array.shape[1] == 3:
                    monthly_data.append(daily_array)
                else:
                    print(f"{s3_key} has no data")
            except Exception as e:
                print(f'Failed to download or load {s3_key}: {e}')
            day += timedelta(days=1)

        if monthly_data:
            monthly_array = np.concatenate(monthly_data, axis=0)
            tmp_file_name = f'/tmp/{device.replace("/", "_")}_monthly.npy'
            print(f"Writing {monthly_array.shape}")
            np.save(tmp_file_name, monthly_array)
            s3_key = f'{device}/{date_str}/data.npy'
            s3.upload_file(tmp_file_name, bucket_name, s3_key)
        else:
            print(f'No data found for device {device} for month {date_str}')

def process_yearly(devices, s3, bucket_name, input_date):
    if input_date is None:
        today = date.today()
        year = today.year - 1
    else:
        specified_date = datetime.strptime(input_date, '%Y-%m-%d').date()
        year = specified_date.year

    date_str = f'{year}'

    for device in devices:
        yearly_data = []

        for month in range(1, 13):
            month_str = f'{year}/{month:02d}'
            s3_key = f'{device}/{month_str}/data.npy'

            try:
                tmp_file_name = f'/tmp/{device.replace("/", "_")}_{year}{month:02d}.npy'
                s3.download_file(bucket_name, s3_key, tmp_file_name)
                monthly_array = np.load(tmp_file_name)
                print(f"Loaded {s3_key} containing {monthly_array.shape}")
                if monthly_array.shape[0] != 0 and monthly_array.shape[1] == 3:
                    yearly_data.append(monthly_array)
                else:
                    print(f"{s3_key} has no data")
            except Exception as e:
                print(f'Failed to download or load {s3_key}: {e}')

        if yearly_data:
            yearly_array = np.concatenate(yearly_data, axis=0)
            tmp_file_name = f'/tmp/{device.replace("/", "_")}_yearly.npy'
            print(f"Writing {yearly_array.shape}")
            np.save(tmp_file_name, yearly_array)
            s3_key = f'{device}/{date_str}/data.npy'
            s3.upload_file(tmp_file_name, bucket_name, s3_key)
        else:
            print(f'No data found for device {device} for year {year}')

