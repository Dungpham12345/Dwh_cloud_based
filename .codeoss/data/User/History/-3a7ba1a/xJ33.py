from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Google Cloud
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound

# khai bao
PROJECT_ID = "bright-voltage-462902-s0"
TEMP_DATASET = "temp_hrms"
ODS_DATASET = "ods_hrms"
DWH_DATASET = "dwh_hrms"
TABLE_QUERY_LOG = "query_log"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

client = bigquery.Client(project=PROJECT_ID)

def create_temp_tables():
    tables = ["employee", "candidates", "department", "job_description", "applyfor", "person_in_charge"]
    for table in tables:
        query = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{TEMP_DATASET}.temp_{table}` AS
        SELECT *
        FROM `{PROJECT_ID}.{ODS_DATASET}.{table}`
        WHERE CAST(JSON_EXTRACT_SCALAR(new_data, '$.{table}_id') AS STRING) IN (
            SELECT JSON_EXTRACT_SCALAR(new_data, '$.{table}_id')
            FROM `{PROJECT_ID}.{ODS_DATASET}.query_log`
            WHERE affected_table = '{table}' AND DATE(query_time) = CURRENT_DATE()
        )
        """
        client.query(query).result()

def transform_and_append():
    append_queries = [
        f"""
        INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.dim_job`
        SELECT DISTINCT opening_id, position_name, status, num_positions, start_date, end_date
        FROM `{PROJECT_ID}.{TEMP_DATASET}.temp_job_description`
        """,
        f"""
        INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.dim_employee`
        SELECT * FROM `{PROJECT_ID}.{TEMP_DATASET}.temp_employee`
        """,
        f"""
        INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.dim_department`
        SELECT * FROM `{PROJECT_ID}.{TEMP_DATASET}.temp_department`
        """,
        f"""
        INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.dim_candidate`
        SELECT DISTINCT candidate_id, candidate_name, status, last_time_stage,
                        source, assign_username, reason, apply_date
        FROM `{PROJECT_ID}.{TEMP_DATASET}.temp_candidates`
        """,
        f"""
        INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.fact_apply_log`
        SELECT * FROM `{PROJECT_ID}.{TEMP_DATASET}.temp_applyfor`
        """
    ]

with DAG(
    dag_id='postgres_scd_to_bq_scd_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 6),
    catchup=False,
) as dag:

    create_temp_tables_task = PythonOperator(
        task_id='create_temp_tables',
        python_callable=create_temp_tables
    )

    append_to_dwh_task = PythonOperator(
        task_id='append_to_dwh',
        python_callable=append_to_dwh
    )

    create_temp_tables_task >> append_to_dwh_task
