import calendar
import json
from datetime import date, datetime, timedelta

import numpy as np
from dao.LocationTable import LocationTable
from dao.MeasurementsBucket import MeasurementsBucket
from dao.MeasurementsTable import MeasurementsTable

# location_table = LocationTable(os.environ['LOCATION_TABLE_NAME'])
# measurements_table = MeasurementsTable(os.environ['MEASUREMENTS_TABLE_NAME'])
# measurements_bucket = MeasurementsBucket(os.environ['BUCKET_NAME'])

location_table = LocationTable("IotSystemCdkStack-DeviceLocations112CC256-1S5L1KJQCC1W3")
measurements_table = MeasurementsTable("MeasurementsTable")
measurements_bucket = MeasurementsBucket("picotherm-measurement-data")

def handler(event, context):
    print(event)
    frequency = event.get('frequency', 'daily').lower()
    input_date = event.get('date')
    if input_date:
        input_date = datetime.strptime(event.get('date'), '%Y-%m-%d')

    devices = ["picotherm/1"]#location_table.get_all_device_ids()

    if frequency == 'daily':
        process_daily(devices, input_date)
    elif frequency == 'monthly':
        process_monthly(devices, input_date)
    elif frequency == 'yearly':
        process_yearly(devices, input_date)
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid frequency parameter.')
        }

    return {
        'statusCode': 200,
        'body': json.dumps(f'Data successfully processed and uploaded for {frequency}.')
    }

def process_daily(devices: list[str], input_date: str | None):
    if input_date is None:
        end = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        end = input_date + timedelta(days=1)
    start = end - timedelta(days=1)

    start_time = int(start.timestamp() * 1000)
    end_time = int(end.timestamp() * 1000)

    for device in devices:
        array = measurements_table.get_sensor_data(device, start_time, end_time)
        measurements_bucket.upload_day(device, start, array)

def process_monthly(devices: list[str], input_date: str | None):
    if input_date is None:
        today = date.today()
        first_day_this_month = date(today.year, today.month, 1)
        last_month_end = first_day_this_month - timedelta(days=1)
        start = date(last_month_end.year, last_month_end.month, 1)
        end = date(last_month_end.year, last_month_end.month, last_month_end.day)
    else:
        start = date(input_date.year, input_date.month, 1)
        end = date(input_date.year, input_date.month, calendar.monthrange(start.year, start.month)[1])

    date_str = start.strftime('%Y/%m')

    for device in devices:
        daily_arrays = []

        day = start
        while day <= end:
            daily_array = measurements_bucket.download_day(device, day)

            if daily_array is not None:
                if daily_array.shape[0] != 0 and daily_array.shape[1] == 3:
                    daily_arrays.append(daily_array)
                else:
                    print(f"Shape of day {day} is incorrect, is {daily_array.shape}")

            day += timedelta(days=1)

        if daily_arrays:
            monthly_array = np.concatenate(daily_arrays, axis=0)
            measurements_bucket.upload_month(device, start, monthly_array)
        else:
            print(f'No data found for device {device} for month {date_str}')

def process_yearly(devices: list[str], input_date: str | None):
    if input_date is None:
        today = date.today()
        year = today.year - 1
    else:
        year = input_date.year

    for device in devices:
        monthly_arrays = []

        for month in range(1, 13):
            month_datetime = date(year, month, 1)
            
            monthly_array = measurements_bucket.download_month(device, month_datetime)

            if monthly_array is not None:
                if monthly_array.shape[0] != 0 and monthly_array.shape[1] == 3:
                    monthly_arrays.append(monthly_array)
                else:
                    print(f"Shape of month {month_datetime} is incorrect, is {monthly_array.shape}")

        if monthly_arrays:
            yearly_array = np.concatenate(monthly_arrays, axis=0)
            measurements_bucket.upload_year(device, date(year, 1, 1), yearly_array)
        else:
            print(f'No data found for device {device} for year {year}')


handler({"frequency": "yearly", "date": "2025-01-25"}, {})