
from datetime import datetime
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key
import numpy as np


dynamodb = boto3.resource("dynamodb")


class MeasurementsTable:

    def __init__(self, table_name: str):
        self.table = dynamodb.Table(table_name)

    def get_sensor_data(self, device: str, start_time: datetime, end_time: datetime):
        response = self.table.query(
            KeyConditionExpression=Key("device_id").eq(device) & Key("time").between(Decimal(start_time), Decimal(end_time))
        )

        items = response.get("Items", []) 

        data = np.array([
            [float(item["time"]), float(item["temperature"]), float(item["humidity"])]
            for item in items
        ], dtype=np.float64)

        return data