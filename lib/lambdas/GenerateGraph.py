from datetime import datetime, timedelta
import hashlib
import io
import os

import boto3
import dateparser
from matplotlib import pyplot as plt
from matplotlib.dates import DateFormatter, HourLocator
import numpy as np

from dao.LocationTable import LocationTable
from dao.MeasurementsTable import MeasurementsTable
from dao.MeasurementsBucket import MeasurementsBucket
from dao.MeasurementHelper import MeasurementHelper

MOVAVG_RADIUS = 3

CORRECT_PASSWORD_HASH = os.environ['PASSWORD_HASH']
LOCATION_TABLE_NAME = os.environ['LOCATION_TABLE_NAME']


def get_output_page(svg, title):
    return f"""
<!DOCTYPE html>
<html>
<head>
  <title>{title}</title>
  <!-- empty icon data to avoid browser making another request -->
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


def moving_average(x, w):
    return np.convolve(x, np.ones(w), 'valid') / w


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
    period_input = event.get("period")

    if None in (password, location, from_input, until_input, period_input):
        return get_error_page("password, location, from, until, and period must be provided.")

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

    reference = datetime(2000, 1, 1)  # Arbitrary fixed point
    parsed_period = dateparser.parse(period_input, settings={"RELATIVE_BASE": reference})
    
    if parsed_period:
        period_seconds = -(parsed_period - reference).total_seconds()
    else:
        return get_error_page("Unable to interpret period.")
    
    device_id = location_table.get_device_id_by_location(location)

    if not device_id:
        return get_error_page(f"Device matching location not found.")
    print("Found devide ID", device_id)

    data = measurements_helper.get_data_in_range(device_id, from_time, until_time)

    print(f"Downloaded data shape is {data.shape}")
    if data.size == 0:
        return get_error_page("No data was found for the given time.")
    
    number_of_points = int((until_time - from_time).total_seconds() // period_seconds)
    moving_average_radius = int(number_of_points // 100)
    print(f"Drawing {number_of_points} points with temperature moving average of {moving_average_radius}.")

    time = np.linspace(data[0, 0], data[-1, 0], number_of_points)

    temperature = np.interp(time, data[:, 0], data[:, 1])
    humidity = np.interp(time, data[:, 0], data[:, 2])
    time = time.astype('datetime64[ms]')

    fig, axis_temperature = plt.subplots(figsize=(12, 7))
    axis_humidity = axis_temperature.twinx()

    color_temperature = plt.get_cmap("viridis")(0)
    color_humidity = plt.get_cmap("viridis")(0.5)

    axis_temperature.set_ylabel("Temperature", color=color_temperature, fontsize=15)
    axis_humidity.set_ylabel("Humidity", color=color_humidity, fontsize=15)

    time_moving_average = time[moving_average_radius:-moving_average_radius] if moving_average_radius > 0 else time
    axis_temperature.plot(
        time_moving_average,
        moving_average(temperature, moving_average_radius * 2 + 1),
        "-",
        color=color_temperature,
        label="Temperature"
    )

    axis_humidity.plot(time, humidity, '-', color=color_humidity, label="Humidity")

    if (until_time - from_time) < timedelta(days=2, hours=1):
        axis_temperature.xaxis.set_major_formatter(DateFormatter('%H:%M'))
    elif (until_time - from_time) < timedelta(days=7, hours=1):
        axis_temperature.xaxis.set_major_formatter(DateFormatter('%d %H:%M'))
    elif (until_time - from_time) < timedelta(weeks=7, hours=1):
        axis_temperature.xaxis.set_major_formatter(DateFormatter('%-m-%d'))
    elif (until_time - from_time) < timedelta(days=30 * 7, hours=1):
        axis_temperature.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    elif (until_time - from_time) < timedelta(days=365 * 7, hours=1):
        axis_temperature.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
    else:
        axis_temperature.xaxis.set_major_formatter(DateFormatter('%Y'))
    
    if (until_time - from_time) < timedelta(days=4, hours=1):
        axis_temperature.xaxis.set_minor_locator(HourLocator())

    if (until_time - from_time) < timedelta(days=7, hours=1):
        midnights = np.unique(time.astype("datetime64[D]"))
        for midnight in midnights:
            if np.min(time) < midnight < np.max(time):
                axis_temperature.axvline(midnight, color="gray", linestyle="--", linewidth=1, alpha=0.7)
    elif (until_time - from_time) < timedelta(weeks=7, hours=1):
        first_day_of_week = np.unique(time.astype("datetime64[W]"))
        for day in first_day_of_week:
            if np.min(time) < day < np.max(time):
                axis_temperature.axvline(day, color="gray", linestyle="--", linewidth=1, alpha=0.7)
    elif (until_time - from_time) < timedelta(days=30 * 7, hours=1):
        first_day_of_month = np.unique(time.astype("datetime64[M]"))
        for day in first_day_of_month:
            if np.min(time) < day < np.max(time):
                axis_temperature.axvline(day, color="gray", linestyle="--", linewidth=1, alpha=0.7)
    elif (until_time - from_time) < timedelta(days=365 * 7, hours=1):
        first_day_of_year = np.unique(time.astype("datetime64[Y]"))
        for day in first_day_of_year:
            if np.min(time) < day < np.max(time):
                axis_temperature.axvline(day, color="gray", linestyle="--", linewidth=1, alpha=0.7)

    plt.title(location.capitalize(), fontsize=20)
    plt.tight_layout()

    f = io.BytesIO()
    plt.savefig(f, format="svg")
    plt.close()

    raw_svg = f.getvalue()

    svg = raw_svg.decode("utf-8")
    svg = svg[svg.find('<svg'):]  # Remove stuff from before the svg
    html = get_output_page(svg, f"{location.capitalize()} from {from_input} until {until_input}")
    return {
        "statusCode": 200,
        "body": html,
        "headers": {
            'Content-Type': 'text/html;charset=utf-8',
        }
    }
