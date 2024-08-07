import json
from google.cloud import bigquery
from google.cloud import storage
import functions_framework
from datetime import date, datetime, timezone
import pytz


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def signal(cloud_event):  
    """Reads a JSON file from the specified path and returns its contents as a Python dictionary."""
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    
    log_filename = f'logs/{date.today()}.txt'   
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    stats = storage.Blob(bucket=bucket, name=log_filename).exists(storage_client)
    
    if stats is True:
        print('Data already inserted for today. Skipping function.')
    else:
        print("Executing full function...")
                
        dataset_id = 'energy_data'
        table_id = 'signal'

        # Create the table if it doesn't exist
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)  
        
        schema = [
            bigquery.SchemaField('datetime', 'DATETIME'),
            bigquery.SchemaField('share', 'FLOAT'),
            bigquery.SchemaField('signal', 'STRING'),
            ]
        
        table = bigquery.Table(table_ref, schema=schema)
        
        try:
            client.get_table(table_ref)  # If it exists, it will not raise an exception
            print(f"Table {table_id} already exists. Skipping creation.")
        except:
            # Create the table if it doesn't exist
            table = client.create_table(table)
            print("Table created")
        
        # Access the file in Cloud Storage using its bucket and name
        storage_client = storage.Client()
        file_obj = bucket.blob(name)

        # Download the file content as a string
        file_content = file_obj.download_as_string()

        # Parse the JSON data
        data = json.loads(file_content)
        
        #convert numbers to descriptions
        signal_dict = {
            -1: "Red (grid congestion)",
            0: "Red (low renewable share)",
            1: "Yellow (average renewable share)",
            2: "Green (high renewable share)"
        }
        
        data['signal'] = [signal_dict[i] for i in data['signal']]

        # Create a list of BigQuery table rows
        rows_to_insert = []
        cet_timezone = pytz.timezone('CET')
        for i in range(len(data['unix_seconds'])): 
            dt_utc = datetime.fromtimestamp(data['unix_seconds'][i], tz=timezone.utc)
            dt_cet = dt_utc.astimezone(cet_timezone).strftime('%Y-%m-%d %H:%M:%S')
            rows_to_insert.append((dt_cet, data['share'][i], data['signal'][i]))
        
        # Insert data into the table
        errors = client.insert_rows(table, rows_to_insert)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
        else:
            print(f"Table {table_ref} populated with {len(rows_to_insert)} rows.")
        
        print("Adding log file")
        
        blob = bucket.blob(f'logs/{date.today()}.txt')
        blob.upload_from_string(' ')
        
        print("Added log file. Closing the function.")