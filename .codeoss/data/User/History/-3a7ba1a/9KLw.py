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
DWH_DATASET = "recruitment_dwh"
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
        INSERT INTO `{project_id}.{dwh_dataset}.dim_job`
        (opening_id, position_name, status, num_positions, start_date, end_date)
        SELECT 
          opening_id,
          position_name,
          CASE CAST(status AS STRING)
            WHEN '10' THEN 'Open'
            WHEN '7' THEN 'Open'
            WHEN '-1' THEN 'Closed'
            ELSE NULL
          END AS status,
          num_positions,
          start_date,
          end_date
        FROM `{project_id}.{temp_dataset}.temp_job_description`;
        """,

        f"""
        INSERT INTO `{project_id}.{dwh_dataset}.dim_employee`
        SELECT * FROM `{project_id}.{temp_dataset}.temp_employee`;
        """,

        f"""
        INSERT INTO `{project_id}.{dwh_dataset}.dim_department`
        SELECT * FROM `{project_id}.{temp_dataset}.temp_department`;
        """,

        f"""
        INSERT INTO `{project_id}.{dwh_dataset}.dim_candidate`
        (candidate_id, candidate_name, status, last_time_stage, source, assign_username, reason, apply_date)
        SELECT 
          candidate_id,
          candidate_name,
          status,
          last_time_stage,
          source,
          assign_username,
          reason,
          apply_date
        FROM `{project_id}.{temp_dataset}.temp_candidates`;
        """,

        f"""
        INSERT INTO `{project_id}.{dwh_dataset}.fact_candidate_log`
        SELECT 
          a.*,
          CASE 
            WHEN a.stage_in_name IS NOT NULL AND a.stage_out_name IS NOT NULL
            THEN EXTRACT(EPOCH FROM (a.stage_out_time - a.stage_in_time)) / 86400
            ELSE NULL
          END AS time_to_respond_minutes,
          CASE
            WHEN a.stage_out_time IS NOT NULL AND a.stage_in_time IS NOT NULL 
              AND EXTRACT(EPOCH FROM (a.stage_out_time - a.stage_in_time)) <= 432000
            THEN TRUE
            ELSE FALSE
          END AS check_sla_on_time
        FROM (
          SELECT 
            af.candidate_id,
            af.opening_id,
            af.stage_in_name,
            af.stage_in_time,
            LEAD(af.stage_in_name) OVER (
              PARTITION BY af.candidate_id, af.opening_id 
              ORDER BY af.stage_in_time
            ) AS stage_out_name,
            LEAD(af.stage_in_time) OVER (
              PARTITION BY af.candidate_id, af.opening_id 
              ORDER BY af.stage_in_time
            ) AS stage_out_time,
            af.note
          FROM `{project_id}.{temp_dataset}.temp_applyfor` af
        ) a;
        """,

        f"""
        INSERT INTO `{project_id}.{dwh_dataset}.fact_pic_tracking`
        SELECT
          pic.employee_code,
          pic.opening_id,
          jd.status,
          jd.num_positions,
          CURRENT_DATE AS snapshot_date,
          js.total_uv,
          js.total_hired,
          js.total_offered,
          js.total_sla_on_time,
          CURRENT_TIMESTAMP AS created_dt,
          CURRENT_TIMESTAMP AS updated_dt
        FROM `{project_id}.{temp_dataset}.temp_person_in_charge` pic
        LEFT JOIN (
            SELECT
              opening_id,
              COUNT(DISTINCT candidate_id) AS total_uv,
              COUNT(DISTINCT CASE WHEN next_stage_name = 'Hired' THEN candidate_id END) AS total_hired,
              COUNT(DISTINCT CASE WHEN next_stage_name = 'Offered' THEN candidate_id END) AS total_offered,
              COUNT(DISTINCT CASE 
                WHEN next_stage_name IS NOT NULL AND stage_in_time IS NOT NULL AND next_stage_time IS NOT NULL 
                  AND EXTRACT(EPOCH FROM (next_stage_time - stage_in_time)) <= 432000
                THEN candidate_id
              END) AS total_sla_on_time
            FROM `{project_id}.{temp_dataset}.temp_applyfor`
            GROUP BY opening_id
        ) js ON pic.opening_id = js.opening_id
        LEFT JOIN `{project_id}.{temp_dataset}.temp_job_description` jd ON pic.opening_id = jd.opening_id;
        """
    ]
    for query in append_queries:
        client.query(query).result()

with DAG(
    dag_id='scd_pipeline',
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
        python_callable=transform_and_append
    )

    create_temp_tables_task >> append_to_dwh_task
