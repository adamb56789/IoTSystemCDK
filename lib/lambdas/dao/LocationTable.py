
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")


class LocationTable:


    def __init__(self, table_name: str):
        self.table = dynamodb.Table(table_name) # type: ignore

    def get_all_device_ids(self) -> list[str]:
        response = self.table.scan()
        
        return [item["device_id"] for item in response.get("Items", [])]
    
    def get_device_id_by_location(self, location: str) -> str | None:
        
        response = self.table.query(
            IndexName="LocationToId",
            KeyConditionExpression=Key("location").eq(location.lower())
        )
        
        items = response.get("Items", [])
        if not items:
            return None
        
        return items[0].get("device_id")