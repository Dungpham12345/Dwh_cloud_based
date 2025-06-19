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

def extract_csv_and_save_avro(bucket_name, gcs_prefix="data/csv_dwh", csv_dir="/home/airflow/gcs/data/csv/"):
    csv_map = {
        "Budget for recruitment.csv": "dim_budget",
        "Cost for recruitment.csv": "fact_cost",
        "Headcount.csv": "fact_headcount"
    }

    today_str = datetime.today().strftime('%Y%m%d')
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for file_name, table_name in csv_map.items():
        file_path = os.path.join(csv_dir, file_name)
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue

        df = pd.read_csv(file_path)

        # Chuẩn hóa kiểu dữ liệu
        for col in df.columns:
            if "date" in col.lower():
                df[col] = pd.to_datetime(df[col], errors="coerce")

        original_types = df.dtypes.astype(str).to_dict()
        process_dataframe(df, original_types)
        schema = get_avro_schema(df, table_name)

        # Save Avro
        avro_path = save_avro_file(df, schema, table_name)

        # Upload to GCS
        gcs_path = f"{gcs_prefix}/{table_name}/{table_name}_{today_str}.avro"
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(avro_path)
        print(f"Uploaded: gs://{bucket_name}/{gcs_path}")
        os.remove(avro_path)
