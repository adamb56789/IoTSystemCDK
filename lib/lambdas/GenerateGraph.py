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

# CORRECT_PASSWORD_HASH = os.environ['PASSWORD_HASH']
# MEASUREMENTS_TABLE_NAME = os.environ['MEASUREMENTS_TABLE_NAME']
# LOCATION_TABLE_NAME = os.environ['LOCATION_TABLE_NAME']
# BUCKET_NAME = os.environ['BUCKET_NAME']

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
    
    locationTable = LocationTable(LOCATION_TABLE_NAME)
    device_id = locationTable.get_device_id_by_location(location)

    
    if not device_id:
        return get_error_page(f"Device matching location not found.")
    print("Found devide ID", device_id)

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

CORRECT_PASSWORD_HASH = hashlib.sha256("password".encode('utf-8')).hexdigest()
MEASUREMENTS_TABLE_NAME = "MeasurementsTable"
LOCATION_TABLE_NAME = "IotSystemCdkStack-DeviceLocations112CC256-1S5L1KJQCC1W3"
BUCKET_NAME = "picotherm-measurement-data"

html = handler({
    "password": "password",
    "location": "Bedroom",
    "from": "yesterday",
    "until": "now"
}, None)["body"]

f = open("test_output.html", "w")
f.write(html)
f.close()

bucket = MeasurementsBucket(BUCKET_NAME)
yesterday = bucket.download_day("picotherm/1", datetime.utcnow() - timedelta(days=1))
