from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Google Cloud
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound

# khai báo
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
def create_dataset_if_not_exists(dataset_name: str, location: str = "asia-southeast1"):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{dataset_name}"
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} đã tồn tại.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        client.create_dataset(dataset)
        print(f"Đã tạo dataset: {dataset_id}")

def create_temp_tables():
    client = bigquery.Client(project=PROJECT_ID)
    create_dataset_if_not_exists(TEMP_DATASET)

    today_suffix = datetime.today().strftime("%Y%m%d")
    prefix_len   = len(today_suffix) + 1  # including the underscore

    tables = client.list_tables(f"{PROJECT_ID}.{ODS_DATASET}")

    for table in tables:
        tbl_id = table.table_id  
        if not tbl_id.endswith(f"_{today_suffix}"):
            continue

        base_name = tbl_id[:-prefix_len]

        sql = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{TEMP_DATASET}.temp_{base_name}` AS
        SELECT * 
        FROM `{PROJECT_ID}.{ODS_DATASET}.{tbl_id}`;
        """
        print(f">>> Running query for temp_{base_name}")
        client.query(sql).result()

def transform_and_append(project_id: str, temp_dataset: str, dwh_dataset: str):

    append_queries = [
        f"""
        INSERT INTO `{project_id}.{dwh_dataset}.dim_job_20250616`
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
        INSERT INTO `{project_id}.{dwh_dataset}.dim_employee_20250616`
        SELECT * FROM `{project_id}.{temp_dataset}.temp_employee`;
        """,

        f"""
        INSERT INTO `{project_id}.{dwh_dataset}.dim_department_20250616`
        SELECT * FROM `{project_id}.{temp_dataset}.temp_department`;
        """,

        f"""
        INSERT INTO `{project_id}.{dwh_dataset}.dim_candidate_20250616`
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
        INSERT INTO `{project_id}.{dwh_dataset}.fact_candidate_log_20250616`
        SELECT 
          a.*,
          CASE 
            WHEN a.stage_in_name IS NOT NULL AND a.stage_out_name IS NOT NULL
            THEN TIMESTAMP_DIFF(CAST(a.stage_out_time AS TIMESTAMP), CAST(a.stage_in_time AS TIMESTAMP), DAY)
            ELSE NULL
          END AS time_to_respond_minutes,
          CASE
            WHEN a.stage_out_time IS NOT NULL AND a.stage_in_time IS NOT NULL 
              AND TIMESTAMP_DIFF(CAST(a.stage_out_time AS TIMESTAMP), CAST(a.stage_in_time AS TIMESTAMP), SECOND) <= 432000
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
        INSERT INTO `{project_id}.{dwh_dataset}.fact_pic_tracking_20250616`
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
        python_callable=transform_and_append,
        op_kwargs={
        'project_id': 'bright-voltage-462902-s0',
        'temp_dataset': 'temp_hrms',
        'dwh_dataset': 'recruitment_dwh'
        },
        dag=dag
    )

    create_temp_tables_task >> append_to_dwh_task
