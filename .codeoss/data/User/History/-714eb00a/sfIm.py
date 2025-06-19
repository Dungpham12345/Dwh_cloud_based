from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import json

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

def process_scd2_for_table(table_name):
    query = f'''
    SELECT old_data, new_data
    FROM `{QUERY_LOG_TABLE}`
    WHERE affected_table = "{table_name}" AND DATE(query_time) = CURRENT_DATE()
    '''
    rows = client.query(query).result()

    for row in rows:
        new_data = json.loads(row['new_data']) if row['new_data'] else None
        old_data = json.loads(row['old_data']) if row['old_data'] else None

        if not new_data:
            continue  # skip deletes

        pk = f"{table_name}_id"
        pk_value = new_data.get(pk)
        if not pk_value:
            continue

        check_sql = f'''
        SELECT * FROM `{PROJECT_ID}.{DWH_DATASET}.dim_{table_name}`
        WHERE {pk} = "{pk_value}" AND is_current = TRUE
        '''
        current_rows = list(client.query(check_sql).result())

        has_changes = False
        if current_rows:
            current_record = dict(current_rows[0])
            for key in new_data:
                if str(new_data.get(key)) != str(current_record.get(key)):
                    has_changes = True
                    break

            if not has_changes:
                continue

            update_sql = f'''
            UPDATE `{PROJECT_ID}.{DWH_DATASET}.dim_{table_name}`
            SET valid_to = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY),
                is_current = FALSE,
                updated_at = CURRENT_TIMESTAMP()
            WHERE {pk} = "{pk_value}" AND is_current = TRUE
            '''
            client.query(update_sql).result()

        columns = list(new_data.keys())
        values = [f'"{str(new_data.get(col))}"' if new_data.get(col) is not None else 'NULL' for col in columns]
        columns += ["valid_from", "valid_to", "is_current", "created_at", "updated_at"]
        values += ["CURRENT_DATE()", "NULL", "TRUE", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP()"]

        insert_sql = f'''
        INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.dim_{table_name}`
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
