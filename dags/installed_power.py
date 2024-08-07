# Imports
from datetime import date, datetime, timedelta
import requests
import json
import os
from google.cloud import storage
import logging
from airflow import models
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import requests


def extract_installed_power(): # task_instance
    # Specify API parameters & query the API 
    parameters = {'country': 'pl', 
                }

    result = requests.get(
        "https://api.energy-charts.info/installed_power?", parameters)

    # If the API call was sucessful, get the data and dump it to a file (first local, then GCP)
    if result.status_code == 200:

        # Get the data
        json_data = result.json()
        data_bytes = json.dumps(json_data).encode("utf-8")
        logging.info("Response from API: {}".format(json_data))
        

        # Pass variables to XCom. TODO delete
        # You'll use them in the next task.
        current_datetime = str(datetime.now().strftime("%m-%d-%Y-%H-%M-%S"))
        file_name = 'installed_power' + "_" + current_datetime + '.json'

        # upload to GCS
        logging.info(f"Will write output to GCS: {file_name}")
        
        client = storage.Client()
        bucket = client.bucket('installed_power_europe')
        blob = bucket.blob(file_name)
        blob.upload_from_string(data_bytes)
        
        logging.info(f"Successfully wrote output file to GCS: gs://{bucket}/{file_name}")
    
    else:
        raise ValueError('"Error In API call."')


    # Specify API parameters & query the API 
    parameters = {'country': 'pl', 
                }

    result = requests.get(
        "https://api.energy-charts.info/signal?", parameters)

    # If the API call was sucessful, get the data and dump it to a file (first local, then GCP)
    if result.status_code == 200:

        # Get the data
        json_data = result.json()
        data_bytes = json.dumps(json_data).encode("utf-8")        

        # Pass variables to XCom. TODO delete
        # You'll use them in the next task.
        current_datetime = str(datetime.now().strftime("%m-%d-%Y-%H-%M-%S"))
        file_name = 'signal' + "_" + current_datetime + '.json'

        # upload to GCS
        logging.info(f"Will write output to GCS: {file_name}")
        
        client = storage.Client()
        bucket = client.bucket('signal_europe')
        blob = bucket.blob(file_name)
        blob.upload_from_string(data_bytes)
        
        logging.info(f"Successfully wrote output file to GCS: gs://{bucket}/{file_name}")
    
    else:
        raise ValueError('"Error In API call."')

# DAG setup
default_args = {
    'owner': 'Szymon',
    'start_date': datetime(2024, 7, 30),
    'retries': 0,
    # 'retry_delay': timedelta(seconds=60)
}

with models.DAG('installed_power',
                default_args=default_args,
                schedule_interval='0 6 1 1 *',
                catchup=False,
                max_active_runs=1) as dag:

    # Set DAGs
    extract_installed_power = PythonOperator(
        task_id='extract_installed_power',
        python_callable=extract_installed_power,
    )
    
    # Pipe DAGs
    [extract_installed_power]