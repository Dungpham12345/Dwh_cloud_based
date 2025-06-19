from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import ast

PROJECT_ID = "bright-voltage-462902-s0"
ODS_DATASET = "ods_hrms"
DWH_DATASET = "recruitment_dwh"
QUERY_LOG_TABLE = f"{PROJECT_ID}.{ODS_DATASET}.query_log"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

client = bigquery.Client(project=PROJECT_ID)
# Tên bảng nguồn → tên bảng đích trong BigQuery
TABLE_NAME_MAPPING = {
    "department": "dim_department",
    "employee": "dim_employee",
    "job_description": "dim_job",
    "candidates": "dim_candidate",
    "applyfor": "fact_candidate_log",  # nếu cần SCD cho bảng fact (hiếm)
    "person_in_charge": "fact_pic_tracking"
}
TABLE_PK_MAPPING = {
    "department": "dept_id",
    "employee": "employee_code",
    "job_description": "opening_id",
    "candidates": "candidate_id",
    "applyfor": "apply_id",
    "person_in_charge": "employee_code"
}

def process_scd2_for_table(table_name):
    bq_table = TABLE_NAME_MAPPING.get(table_name)
    pk = TABLE_PK_MAPPING.get(table_name)
    if not bq_table or not pk:
        raise ValueError(f"Mapping không tồn tại cho {table_name}")

    query = f'''
    SELECT old_data, new_data
    FROM `{QUERY_LOG_TABLE}`
    WHERE affected_table = "{table_name}" AND DATE(query_time) = CURRENT_DATE()
    '''
    rows = client.query(query).result()

    for row in rows:
        new_data = ast.literal_eval(row['new_data']) if row['new_data'] else None
        old_data = ast.literal_eval(row['old_data']) if row['old_data'] else None
        if not new_data:
            continue  # skip DELETE

        pk_value = new_data.get(pk)
        if not pk_value:
            continue

        check_sql = f'''
        SELECT * FROM `{PROJECT_ID}.{DWH_DATASET}.{bq_table}`
        WHERE {pk} = "{pk_value}" AND is_current = TRUE
        '''
        current_rows = list(client.query(check_sql).result())
        has_changes = True

        if current_rows:
            current_record = dict(current_rows[0])
            has_changes = any(str(new_data.get(k)) != str(current_record.get(k)) for k in new_data)

            if not has_changes:
                continue

            update_sql = f'''
            UPDATE `{PROJECT_ID}.{DWH_DATASET}.{bq_table}`
            SET valid_to = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY),
                is_current = FALSE,
                updated_at = CURRENT_TIMESTAMP()
            WHERE {pk} = "{pk_value}" AND is_current = TRUE
            '''
            client.query(update_sql).result()

        columns = list(new_data.keys()) + ["valid_from", "valid_to", "is_current", "created_at", "updated_at"]
        values = [f'"{new_data.get(col)}"' if new_data.get(col) is not None else 'NULL' for col in new_data.keys()] + \
                 ["CURRENT_DATE()", "NULL", "TRUE", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP()"]

        insert_sql = f'''
        INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.{bq_table}`
        ({", ".join(columns)})
        VALUES ({", ".join(values)})
        '''
        client.query(insert_sql).result()


with DAG(
    dag_id="scd2_all_hrm_tables",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
) as dag:

    table_list = ['employee', 'department', 'job_description', 'candidates', 'applyfor', 'person_in_charge']

    for tbl in table_list:
        PythonOperator(
            task_id=f"scd2_process_{tbl}",
            python_callable=process_scd2_for_table,
            op_args=[tbl]
        )
