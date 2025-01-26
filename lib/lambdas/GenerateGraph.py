from datetime import datetime, timedelta
import hashlib
import io
import os

import boto3
import dateparser
from matplotlib import pyplot as plt

from dao.LocationTable import LocationTable
from dao.MeasurementsTable import MeasurementsTable
from dao.MeasurementsBucket import MeasurementsBucket
from dao.MeasurementHelper import MeasurementHelper

CORRECT_PASSWORD_HASH = os.environ['PASSWORD_HASH']
LOCATION_TABLE_NAME = os.environ['LOCATION_TABLE_NAME']

def get_output_page(svg):
    return f"""
<!DOCTYPE html>
<html>
<head>
  <title>Home Measurement History</title>
  <!-- icon data to avoid browser making another request -->
  <link rel="icon" href="data:,">
</head>

<body>
{svg}
</body>
</html>
"""

def get_error_page(error_message):
    print("400 error:", error_message)
    return {
        "statusCode": 400,
        "body": error_message,
        "headers": {
            'Content-Type': 'text/html;charset=utf-8',
        }
    }

dynamodb = boto3.resource("dynamodb")
location_table = LocationTable(LOCATION_TABLE_NAME)
measurements_helper = MeasurementHelper(MeasurementsTable(os.environ['MEASUREMENTS_TABLE_NAME']), MeasurementsBucket(os.environ['BUCKET_NAME']))


def handler(event, context):
    print("Received event:", event)
    
    # If it is a lambda url extract the url params
    if 'queryStringParameters' in event:
        event = event['queryStringParameters']

    password = event.get("password")
    location = event.get("location")
    from_input = event.get("from")
    until_input = event.get("until")

    if None in (password, location, from_input, until_input):
        return get_error_page("password, location, from, and until must be provided.")

    hash = hashlib.sha256(password.encode('utf-8')).hexdigest()

    if hash != CORRECT_PASSWORD_HASH:
        return get_error_page("Password is incorrect.")

    from_time = dateparser.parse(from_input)
    until_time = dateparser.parse(until_input)

    print(f"Interpreted dates as {from_time} to {until_time}")

    if from_time is None or until_time is None:
        return get_error_page("Unable to interpret date and time from 'from' or 'until'.")

    if from_time >= until_time:
        return get_error_page("'from' date must be earlier than 'until' date.")
    
    device_id = location_table.get_device_id_by_location(location)

    if not device_id:
        return get_error_page(f"Device matching location not found.")
    print("Found devide ID", device_id)

    data = measurements_helper.get_data_in_range(device_id, from_time, until_time)

    print(f"Downloaded data shaps is {data.shape}")
    if data.size == 0:
        return get_error_page("No data was found for the given time.")
    plt.plot(data[:, 0].astype('datetime64[ms]'), data[:, 1])

    plt.title(location)
    plt.tight_layout()

    f = io.BytesIO()
    plt.savefig(f, format="svg")
    plt.close()

    raw_svg = f.getvalue()

    svg = raw_svg.decode("utf-8")
    svg = svg[svg.find('<svg'):]  # Remove stuff from front before the svg
    html = get_output_page(svg)
    return {
        "statusCode": 200,
        "body": html,
        "headers": {
            'Content-Type': 'text/html;charset=utf-8',
        }
    }
