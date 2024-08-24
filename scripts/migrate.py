import datetime
import boto3
from botocore.exceptions import ClientError

def str_to_epoch(time_str):
    # Convert a time string to epoch milliseconds
    time_str = time_str[:26]
    dt_obj = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')
    millisec = int(dt_obj.timestamp() * 1000)
    return millisec

def migrate_temperature_to_dynamodb(timestream_database, timestream_table, dynamodb_table_name):
    # Initialize Timestream and DynamoDB clients
    timestream_query_client = boto3.client('timestream-query')
    dynamodb_client = boto3.client('dynamodb')
    
    # Query to fetch temperature data from the Timestream table
    query = f"SELECT device, time, measure_name, measure_value::double FROM {timestream_database}.{timestream_table} WHERE measure_name = 'temperature'"
    next_token = None  # Initialize next_token for pagination

    # Execute query in Timestream and handle pagination
    try:
        while True:
            query_params = {'QueryString': query}
            if next_token:
                query_params['NextToken'] = next_token

            query_response = timestream_query_client.query(**query_params)
            next_token = query_response.get('NextToken')

            # Process the rows
            for record in query_response['Rows']:
                device_id = record['Data'][0]['ScalarValue']
                time_value = str_to_epoch(record['Data'][1]['ScalarValue'])
                measure_value = record['Data'][3]['ScalarValue']

                # Preparing DynamoDB item
                item = {
                    'device_id': {'S': device_id},
                    'time': {'N': str(time_value)},
                    'payload': {'M': {'temperature': {'N': str(measure_value)}}}
                }

                # Write data to DynamoDB
                try:
                    dynamodb_client.put_item(TableName=dynamodb_table_name, Item=item)
                    print(f"Data written to DynamoDB for device {device_id} at time {time_value}")
                except ClientError as e:
                    print(f"Failed to write data to DynamoDB: {e}")

            if not next_token:
                break  # Exit loop if no more data

    except ClientError as e:
        print(f"An error occurred querying Timestream: {e}")

def update_humidity_to_dynamodb(timestream_database, timestream_table, dynamodb_table_name):
    # Initialize Timestream and DynamoDB clients
    timestream_query_client = boto3.client('timestream-query')
    dynamodb_client = boto3.client('dynamodb')
    
    # Query to fetch humidity data from the Timestream table
    query = f"SELECT device, time, measure_name, measure_value::double FROM {timestream_database}.{timestream_table} WHERE measure_name = 'humidity'"
    next_token = None  # Initialize next_token for pagination

    # Execute query in Timestream and handle pagination
    try:
        while True:
            query_params = {'QueryString': query}
            if next_token:
                query_params['NextToken'] = next_token

            query_response = timestream_query_client.query(**query_params)
            next_token = query_response.get('NextToken')

            # Process the rows
            for record in query_response['Rows']:
                device_id = record['Data'][0]['ScalarValue']
                time_value = str_to_epoch(record['Data'][1]['ScalarValue'])
                measure_value = record['Data'][3]['ScalarValue']

                # Prepare the update expression to add humidity to the payload
                update_expression = 'SET payload.humidity = :val'
                expression_attribute_values = {
                    ':val': {'N': str(measure_value)}
                }

                # Update data in DynamoDB
                try:
                    dynamodb_client.update_item(
                        TableName=dynamodb_table_name,
                        Key={
                            'device_id': {'S': device_id},
                            'time': {'N': str(time_value)}
                        },
                        UpdateExpression=update_expression,
                        ExpressionAttributeValues=expression_attribute_values
                    )
                    print(f"Updated DynamoDB for device {device_id} at time {time_value} with humidity {measure_value}")
                except ClientError as e:
                    print(f"Failed to update data in DynamoDB: {e}")

            if not next_token:
                break  # Exit loop if no more data

    except ClientError as e:
        print(f"An error occurred querying Timestream: {e}")

update_humidity_to_dynamodb('IOT_DB', 'measurements', 'MeasurementsTable')