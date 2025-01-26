
from datetime import datetime
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key
import numpy as np


dynamodb = boto3.resource("dynamodb")


class MeasurementsTable:

    def __init__(self, table_name: str):
        self.table = dynamodb.Table(table_name) # type: ignore

    def get_sensor_data(self, device: str, start_time: datetime, end_time: datetime) -> np.ndarray:
        start = int(start_time.timestamp() * 1000)
        end = int(end_time.timestamp() * 1000)
        
        response = self.table.query(
            KeyConditionExpression=Key("device_id").eq(device) & Key("time").between(Decimal(start), Decimal(end))
        )

        items = response.get("Items", []) 

        data = np.array([
            [float(item["time"]), float(item["temperature"]), float(item["humidity"])]
            for item in items
        ], dtype=np.float64)

        return data