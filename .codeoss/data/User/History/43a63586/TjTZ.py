from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pandas as pd
import os

#default config
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

#extract csv file
def extract_csv(**kwargs):
    csv_folder = '/home/airflow/gcs/data/csv/'
    all_dataframes = []

    for filename in os.listdir(csv_folder):
        if filename.endswith('.csv'):
            file_path = os.path.join(csv_folder, filename)
            df = pd.read_csv(file_path)
            all_dataframes.append(df)
            print(f"Extracted {filename} with {len(df)} rows.")
    print(f"Total rows extracted: {sum(len(df) for df in all_dataframes)}")
