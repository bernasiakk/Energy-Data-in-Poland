import json
from google.cloud import bigquery
from google.cloud import storage
import functions_framework
from datetime import date, datetime, timezone
import pytz



# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def public_power(cloud_event): #TODO
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
    table_id = 'public_power'
    
    # Create the table if it doesn't exist
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)

    schema = [
      bigquery.SchemaField('datetime', 'DATETIME'),
      bigquery.SchemaField('production_type', 'STRING'),
      bigquery.SchemaField('value', 'FLOAT'),
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

    # Create a list of BigQuery table rows
    rows_to_insert = []
    cet_timezone = pytz.timezone('CET')
    for i, timestamp in enumerate(data['unix_seconds']):
      dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
      dt_cet = dt_utc.astimezone(cet_timezone).strftime('%Y-%m-%d %H:%M:%S')
      for production_type in data['production_types']:
        rows_to_insert.append((dt_cet, production_type['name'], production_type['data'][i]))

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