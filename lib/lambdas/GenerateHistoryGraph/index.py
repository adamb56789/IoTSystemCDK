import boto3
import json
import io
import zlib
import hashlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, HourLocator
import os

OUTPUT_FORMATS = ["raw_text_svg", "compressed_svg"]

DATA_RESOLUTION = 300
MOVAVG_RADIUS = 3

CORRECT_PASSWORD_HASH = os.environ['CORRECT_PASSWORD_HASH']
TIMESTREAM_DB_NAME = os.environ['TIMESTREAM_DB_NAME']
TIMESTREAM_TABLE_NAME = os.environ['TIMESTREAM_TABLE_NAME']


def get_query(device_name, graph_duration):
    return f'''SELECT measure_name,
    INTERPOLATE_LINEAR(
      CREATE_TIME_SERIES(time, measure_value::double), 
      SEQUENCE(min(time), max(time), (max(time) - min(time))/(300 - 1))
    ) AS series
  FROM "{TIMESTREAM_DB_NAME}"."{TIMESTREAM_TABLE_NAME}"
  WHERE device='{device_name}'
    AND time BETWEEN ago({graph_duration}) AND now()
  GROUP BY measure_name
  ORDER BY measure_name DESC
  '''


def moving_average(x, w):
    return np.convolve(x, np.ones(w), 'valid') / w


def add_plot(response, device_name, time_mode, show_absolute_humidity):
    rows = response['Rows']

    if len(rows) != 2:
        raise Exception(
            "Received wrong number of measures from timestream: " + str(len(rows)))

    timeseries = rows[0]['Data'][1]['TimeSeriesValue']
    start_time = timeseries[0]['Time']
    end_time = timeseries[-1]['Time']
    time = pd.date_range(start_time, end_time, DATA_RESOLUTION).values
    temperature = np.array([d['Value']['ScalarValue']
                           for d in rows[0]['Data'][1]['TimeSeriesValue']]).astype("single")
    humidity = np.array([d['Value']['ScalarValue']
                        for d in rows[1]['Data'][1]['TimeSeriesValue']]).astype("single")

    fig, ax_t = plt.subplots(figsize=(8, 5))
    ax_rh = ax_t.twinx()

    color_t = plt.cm.viridis(0)
    color_rh = plt.cm.viridis(0.5)

    ax_t.set_ylabel("Temperature", color=color_t)
    ax_rh.set_ylabel("Humidity", color=color_rh)

    ax_t.plot_date(time[MOVAVG_RADIUS:-MOVAVG_RADIUS], moving_average(
        temperature, MOVAVG_RADIUS*2+1), "-", color=color_t, label="Temperature")
    ax_rh.plot_date(time, humidity, '-', color=color_rh, label="Humidity")

    if show_absolute_humidity == "True":
        absolute_humidity = (6.112*np.exp((17.67*temperature) /
                             (temperature+243.5))*humidity*2.1674)/(273.15+temperature)
        ax_ah = ax_t.twinx()
        color_ah = plt.cm.viridis(.9)
        ax_ah.set_ylabel("Absolute Humidity", color=color_ah)
        ax_ah.plot_date(time, absolute_humidity, '-',
                        color=color_ah, label="Absolute Humidity")
        ax_ah.spines['right'].set_position(('outward', 60))
        plt.title("wtf")
        return

    if time_mode == "h":
        ax_t.xaxis.set_major_formatter(DateFormatter('%H:%M'))
        ax_t.xaxis.set_minor_locator(HourLocator())
    elif time_mode == "d":
        ax_t.xaxis.set_major_formatter(DateFormatter('%m-%d'))

    ax_t.grid(axis='x', which='both', linestyle='--')

    plt.title(device_name)
    plt.tight_layout()


def check_param(param, valid):
    if param not in valid:
        raise ValueError(f"Format incorrect: {param} not in {valid}")


client = boto3.client('timestream-query')


def handler(event, context):
    # If is a lambda url extract the url params
    if 'queryStringParameters' in event:
        event = event['queryStringParameters']

    password = event['password']
    output_format = event['output_format']
    device_name = event['device_name']
    graph_duration = event['graph_duration']
    show_absolute_humidity = event['show_absolute_humidity']

    hash = hashlib.sha256(password.encode('utf-8')).hexdigest()

    if hash != CORRECT_PASSWORD_HASH:
        raise Exception("Incorrect password")

    print("Received event: " + json.dumps(event, indent=2))

    check_param(output_format, OUTPUT_FORMATS)

    response = client.query(QueryString=get_query(device_name, graph_duration))

    add_plot(response, device_name, graph_duration[-1], show_absolute_humidity)

    f = io.BytesIO()
    plt.savefig(f, format="svg")
    plt.close()

    raw_svg = f.getvalue()

    if output_format == "raw_text_svg":
        return raw_svg.decode("utf-8")
    else:
        return zlib.compress(raw_svg)
