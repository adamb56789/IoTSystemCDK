import boto3
from concurrent.futures import ThreadPoolExecutor


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('MeasurementsTable')

def fetch_items_with_payload():
    items = []
    last_evaluated_key = None

    while True:
        # Configure the scan operation to handle pagination
        scan_kwargs = {
            'FilterExpression': 'attribute_exists(payload)'
        }
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

        response = table.scan(**scan_kwargs)
        items.extend(response['Items'])

        # Break the loop if no more items are left to fetch
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break

    return items

def update_item(item):
    try:
        table.update_item(
            Key={
                'device_id': item['device_id'],
                'time': item['time']
            },
            UpdateExpression="SET temperature = :temp, humidity = :hum REMOVE payload",
            ExpressionAttributeValues={
                ':temp': item['payload']['temperature'],
                ':hum': item['payload']['humidity']
            },
            ReturnValues="UPDATED_NEW"
        )
        print(f"Updated item {item['device_id']} at {item['time']}")
    except Exception as e:
        print(f"Error updating item {item['device_id']} at {item['time']}: {str(e)}")

def process_items(items):
    # Use ThreadPoolExecutor to update items in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(update_item, items)

def main():
    items = fetch_items_with_payload()
    if items:
        process_items(items)
    else:
        print("No items with 'payload' found.")

if __name__ == '__main__':
    main()
