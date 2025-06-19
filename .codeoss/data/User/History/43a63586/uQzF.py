# DAG để load CSV -> ODS (raw) -> Transform -> DWH
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import storage, bigquery
from fastavro import writer
from fastavro.schema import parse_schema
from datetime import datetime
import os, shutil, pandas as pd

# Const
BUCKET = 'asia-southeast1-datahub-com-9e823cf2-bucket'
PROJECT = 'bright-voltage-462902-s0'
ODS = 'ods_hrms'
DWH = 'recruitment_dwh'
CSV_GCS_FOLDER = 'csv_source'  # gs://bucket/csv_source/*.csv
LOCAL_TEMP = '/home/airflow/gcs/data/tmp_csv/'

def extract_csv_and_upload_to_ods():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)
    blobs = bucket.list_blobs(prefix=CSV_GCS_FOLDER)

    os.makedirs(LOCAL_TEMP, exist_ok=True)
    today = datetime.today().strftime("%Y%m%d")

    for blob in blobs:
        if blob.name.endswith('.csv'):
            filename = os.path.basename(blob.name).replace(' ', '_').replace('.csv', '')
            local_path = os.path.join(LOCAL_TEMP, filename + '.csv')
            blob.download_to_filename(local_path)

            df = pd.read_csv(local_path)
            avro_path = os.path.join(LOCAL_TEMP, f"{filename}_{today}.avro")
            schema = parse_schema({
                "name": filename,
                "type": "record",
                "fields": [
                    {"name": col, "type": ["null", "string"]} for col in df.columns
                ]
            })
            records = df.where(pd.notnull(df), None).to_dict(orient='records')
            with open(avro_path, 'wb') as f:
                writer(f, schema, records)

            gcs_path = f"data/staging/csv/{filename}/{filename}_{today}.avro"
            bucket.blob(gcs_path).upload_from_filename(avro_path)

    shutil.rmtree(LOCAL_TEMP)

def transform_csv_budget():
    df = pd.read_csv('/home/airflow/gcs/data/csv/Budget_for_recruitment.csv')
    df.columns = ['stt', 'budget_id', 'team', 'group', 'channel', 'item', 'quantity', 'unit_price', 'budget_2024', 'package_info']
    return df

def transform_csv_cost():
    df = pd.read_csv('/home/airflow/gcs/data/csv/Cost_for_recruitment.csv')
    df.columns = ['budget_id', 'year', 'used_month', 'payment_month', 'invoice_date', 'invoice_no', 'detail', 'cost', 'total_cost', 'note', 'org_group', 'channel', 'actual_cost']
    return df

def transform_csv_headcount():
    df = pd.read_csv('/home/airflow/gcs/data/csv/Headcount.csv')
    df.columns = ['dept_id', 'layer2', 'layer3', 'layer4', 'year', 'status', 'headcount', 'report_date']
    return df

def save_transformed_to_gcs():
    today = datetime.today().strftime("%Y%m%d")
    def save(df, name):
        df = df.astype(str).where(pd.notnull(df), None)
        schema = parse_schema({
            "name": name,
            "type": "record",
            "fields": [
                {"name": col, "type": ["null", "string"]} for col in df.columns
            ]
        })
        file_path = f"/home/airflow/gcs/data/{name}_{today}.avro"
        with open(file_path, 'wb') as f:
            writer(f, schema, df.to_dict(orient='records'))

        storage.Client().bucket(BUCKET).blob(f"data/dwh/{name}/{name}_{today}.avro").upload_from_filename(file_path)

    save(transform_csv_budget(), 'dim_budget')
    save(transform_csv_cost(), 'fact_cost')
    save(transform_csv_headcount(), 'fact_headcount')

def create_dataset_if_needed(dataset_name):
    client = bigquery.Client(project=PROJECT)
    dataset_id = f"{PROJECT}.{dataset_name}"
    try:
        client.get_dataset(dataset_id)
    except:
        ds = bigquery.Dataset(dataset_id)
        ds.location = "asia-southeast1"
        client.create_dataset(ds)

with DAG(
    dag_id='csv_to_dwh_etl',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_csv_to_ods = PythonOperator(
        task_id='extract_csv_and_upload_to_ods',
        python_callable=extract_csv_and_upload_to_ods
    )

    create_ods = PythonOperator(
        task_id='create_ods_dataset',
        python_callable=create_dataset_if_needed,
        op_kwargs={'dataset_name': ODS}
    )

    ods_load_tasks = []
    today = datetime.today().strftime("%Y%m%d")
    for table in ['budget_for_recruitment', 'Cost_for_recruitment', 'Headcount']:
        base = table.replace('.csv','').replace(' ','_')
        gcs_path = f"data/staging/csv/{base}/{base}_{today}.avro"
        ods_load_tasks.append(
            GCSToBigQueryOperator(
                task_id=f'load_ods_{base}',
                bucket=BUCKET,
                source_objects=[gcs_path],
                destination_project_dataset_table=f"{PROJECT}.{ODS}.{base}",
                source_format='AVRO',
                autodetect=True,
                write_disposition='WRITE_TRUNCATE'
            )
        )

    transform_and_save = PythonOperator(
        task_id='transform_and_save_to_gcs',
        python_callable=save_transformed_to_gcs
    )

    create_dwh = PythonOperator(
        task_id='create_dwh_dataset',
        python_callable=create_dataset_if_needed,
        op_kwargs={'dataset_name': DWH}
    )

    dwh_load_tasks = []
    for tbl in ['dim_budget', 'fact_cost', 'fact_headcount']:
        gcs_path = f"data/dwh/{tbl}/{tbl}_{today}.avro"
        dwh_load_tasks.append(
            GCSToBigQueryOperator(
                task_id=f'load_dwh_{tbl}',
                bucket=BUCKET,
                source_objects=[gcs_path],
                destination_project_dataset_table=f"{PROJECT}.{DWH}.{tbl}",
                source_format='AVRO',
                autodetect=True,
                write_disposition='WRITE_TRUNCATE'
            )
        )

    extract_csv_to_ods >> create_ods >> ods_load_tasks >> transform_and_save >> create_dwh >> dwh_load_tasks
