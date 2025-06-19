from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException
from datetime import datetime
from pathlib import Path
import pandas as pd
import os
import shutil
from sqlalchemy import create_engine
from google.cloud import storage, bigquery
from fastavro import writer
from fastavro.schema import parse_schema
from google.api_core.exceptions import NotFound

# Constants
BUCKET_NAME = 'asia-southeast1-datahub-com-9e823cf2-bucket'
PROJECT_ID = 'bright-voltage-462902-s0'
SCHEMA_NAME = 'hrms'
ODS_DATASET = 'ods_hrms'
DWH_DATASET = 'recruitment_dwh'

# PostgreSQL connection
from airflow.hooks.postgres_hook import PostgresHook
def connect_to_postgres_hook():
    try:
        hook = PostgresHook(postgres_conn_id='postgresql_conn')
        return hook.get_sqlalchemy_engine()
    except Exception as e:
        raise AirflowFailException(f"Database engine init failed: {e}")

# Extract tables list
def checking_data_from_postgrest(engine, schema_name):
    query = f"""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = '{schema_name}' AND table_type = 'BASE TABLE'
    """
    df = pd.read_sql(query, engine)
    print(f"Found tables: {df['table_name'].tolist()}")
    return df
    
def extract_data_postgres(engine, table_name, schema_name):
    query = f'SELECT * FROM {schema_name}."{table_name}"'
    df = pd.read_sql(query, engine)
    return df

# Extract full and save to GCS as Avro
def extract_all_and_upload_to_gcs(bucket_name, schema_name="hrms", output_dir="./output", gcs_prefix="data/staging"):
    engine = connect_to_postgres_hook()
    table_df = checking_data_from_postgrest(engine, schema_name)
    if table_df.empty:
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for _, row in table_df.iterrows():
        table = row['table_name']
        df = pd.read_sql(f'SELECT * FROM {schema_name}."{table}"', engine)
        if df.empty:
            continue

        original_types = df.dtypes.astype(str).to_dict()
        for col in df.columns:
            if original_types[col] == 'object':
                df[col] = df[col].astype(str).replace({'nan': None, 'None': None, '': None})
            elif original_types[col] == 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S')
                df[col] = df[col].where(df[col].notnull(), None)

        schema = get_avro_schema(df, table)
        today_str = datetime.today().strftime('%Y%m%d')
        file_path = os.path.join(output_dir, f"{table}_{today_str}.avro")
        os.makedirs(output_dir, exist_ok=True)

        df_copy = df.copy()
        for col in df_copy.columns:
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]) or isinstance(df_copy[col].iloc[0], datetime):
                df_copy[col] = df_copy[col].apply(
                    lambda x: int(pd.Timestamp(x).timestamp() * 1_000_000) if pd.notnull(x) else None)

        records = df_copy.to_dict(orient="records")
        with open(file_path, "wb") as out:
            writer(out, schema, records)

        gcs_path = f"{gcs_prefix}/{schema_name}/{table}/{table}_{today_str}.avro"
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(file_path)
        os.remove(file_path)

    if os.path.exists(output_dir) and not os.listdir(output_dir):
        shutil.rmtree(output_dir)

# Gen avro schema
def get_avro_schema(df, table_name):
    dtype_to_avro = {
        'object': 'string',
        'int64': 'long',
        'float64': 'double',
        'bool': 'boolean',
        'datetime64[ns]': {"type": "long", "logicalType": "timestamp-micros"},
        'category': 'string',
    }

    fields = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        avro_type = dtype_to_avro.get(dtype, 'string')
        if df[col].isnull().any():
            avro_type = [avro_type, "null"] if not isinstance(avro_type, list) else avro_type
        fields.append({"name": col, "type": avro_type})

    return parse_schema({"name": table_name, "type": "record", "fields": fields})

# BigQuery

def create_bigquery_dataset_if_not_exists(project_id, dataset_name, location="asia-southeast1"):
    client = bigquery.Client(project=project_id)
    dataset_id = f"{project_id}.{dataset_name}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        client.create_dataset(dataset)

# GCS utils
def list_gcs_avro_files(bucket_name, prefix="data/dwh"):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs if blob.name.endswith(".avro")]

# transform function
def transform_dim_job_table():
    # lấy bảng job_opening từ schema hrms
    df_dim_job = extract_data_postgres(connect_to_postgres_hook()
, 'job_description', 'hrms')
    #using .loc to select specific columns
    df_dim_job = df_dim_job.loc[:, ['opening_id', 'position_name', 'status', 'num_positions', 'start_date', 'end_date']]
    #convert start_date and end_date to datetime type
    for col in ['start_date', 'end_date']:
        df_dim_job[col] = pd.to_datetime(df_dim_job[col])
    status_map = {
    "10": "Open",
    "7": "Open",
    "-1": "Closed",
    }
    df_dim_job['status'] = df_dim_job['status'].astype(str).map(status_map)
    return df_dim_job


def transform_dim_employee_table():
    df_dim_employee = extract_data_postgres(connect_to_postgres_hook()
, 'employee', 'hrms')
    # convert onboard_date and quit_date to datetime type
    for col in ['onboard_date', 'quit_date']:
        df_dim_employee[col] = pd.to_datetime(df_dim_employee[col])
    return df_dim_employee

def transform_dim_department_table():
    df_dim_department = extract_data_postgres(connect_to_postgres_hook()
, 'department', 'hrms')
    return df_dim_department

def transform_dim_candidate_table():
    df_dim_candidate = extract_data_postgres(connect_to_postgres_hook()
, 'candidates', 'hrms')
    # select specific columns
    df_dim_candidate = df_dim_candidate.loc[:, ['candidate_id', 'candidate_name', 'status', 'last_time_stage', 'source', 'assign_username', 'reason', 'apply_date']]
    # convert apply_date to datetime type
    df_dim_candidate['apply_date'] = pd.to_datetime(df_dim_candidate['apply_date'])
    return df_dim_candidate

def transform_fact_candidate_log_tracking_table():
    engine = connect_to_postgres_hook()

    query = """
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
        FROM hrms.applyfor af
    ) a
    """

    df_fact_candidate_log = pd.read_sql(query, engine)
    return df_fact_candidate_log

def transform_fact_pic_tracking_table():
    engine = connect_to_postgres_hook()

    query = """
    WITH job_level_summary AS (
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
    FROM hrms.applyfor
    GROUP BY opening_id
    )
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
    FROM hrms.person_in_charge pic
    LEFT JOIN job_level_summary js ON pic.opening_id = js.opening_id
    LEFT JOIN hrms.job_description jd ON pic.opening_id = jd.opening_id
    order by pic.opening_id;
    """
    df_fact_pic_tracking = pd.read_sql(query, engine)
    return df_fact_pic_tracking
def save_dim_and_fact_tables_to_gcs(bucket=BUCKET_NAME, output_dir="./output", gcs_prefix="data/dwh"):
    today_str = datetime.today().strftime("%Y%m%d")

    def save_upload(df: pd.DataFrame, table_name: str):
        # Bước 1: Làm sạch kiểu dữ liệu
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors='coerce')

            elif df[col].dtype == 'object':
                non_null_values = df[col].dropna()
                if not non_null_values.empty and isinstance(non_null_values.iloc[0], datetime):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                else:
                    df[col] = df[col].astype(str).replace({'nan': None, 'None': None, '': None})

        # Bước 2: Tạo schema Avro đúng kiểu timestamp
        schema = get_avro_schema(df, table_name)

        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, f"{table_name}_{today_str}.avro")

        # Bước 3: Chuyển datetime sang timestamp-micros
        df_copy = df.copy()
        for col in df_copy.columns:
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]) or (
                df_copy[col].dtype == 'object' and df_copy[col].dropna().apply(lambda x: isinstance(x, datetime)).any()
            ):
                df_copy[col] = df_copy[col].apply(
    lambda x: int(x.timestamp() * 1_000_000) if isinstance(x, datetime) else None
)


        records = df_copy.to_dict(orient="records")

        with open(file_path, "wb") as out:
            writer(out, schema, records)

        gcs_path = f"{gcs_prefix}/{table_name}/{table_name}_{today_str}.avro"
        storage.Client().bucket(bucket).blob(gcs_path).upload_from_filename(file_path)
        os.remove(file_path)
        print(f"Uploaded gs://{bucket}/{gcs_path}")

    # Gọi các hàm transform
    save_upload(transform_dim_job_table(), 'dim_job')
    save_upload(transform_dim_employee_table(), 'dim_employee')
    save_upload(transform_dim_department_table(), 'dim_department')
    save_upload(transform_dim_candidate_table(), 'dim_candidate')
    save_upload(transform_fact_candidate_log_tracking_table(), 'fact_candidate_log')
    save_upload(transform_fact_pic_tracking_table(), 'fact_pic_tracking')

    # Xóa thư mục output nếu rỗng
    if os.path.exists(output_dir) and not os.listdir(output_dir):
        shutil.rmtree(output_dir)

# DAG
with DAG(
    dag_id='test_etl_dag',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'optimized']
) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=checking_data_from_postgrest,
        op_kwargs={'engine': connect_to_postgres_hook(), 'schema_name': SCHEMA_NAME}
    )

    extract_and_upload = PythonOperator(
        task_id='extract_and_upload_all_tables_to_gcs',
        python_callable=extract_all_and_upload_to_gcs,
        op_kwargs={'bucket_name': BUCKET_NAME, 'schema_name': SCHEMA_NAME}
    )

    create_ods_dataset = PythonOperator(
        task_id='create_ods_bigquery_dataset',
        python_callable=create_bigquery_dataset_if_not_exists,
        op_kwargs={'project_id': PROJECT_ID, 'dataset_name': ODS_DATASET}
    )

    with TaskGroup("ods_load_tasks_group") as ods_load_tasks:
        for blob_path in list_gcs_avro_files(BUCKET_NAME, prefix=f"data/staging/{SCHEMA_NAME}"):
            table = Path(blob_path).stem
            GCSToBigQueryOperator(
                task_id=f"load_ods_{table}_to_bq",
                bucket=BUCKET_NAME,
                source_objects=[blob_path],
                destination_project_dataset_table=f"{PROJECT_ID}.{ODS_DATASET}.{table}",
                source_format='AVRO',
                write_disposition='WRITE_TRUNCATE',
                autodetect=True,
                create_disposition='CREATE_IF_NEEDED'
            )

    transform_funcs = {
        'dim_job': transform_dim_job_table,
        'dim_employee': transform_dim_employee_table,
        'dim_department': transform_dim_department_table,
        'dim_candidate': transform_dim_candidate_table,
        'fact_candidate_log': transform_fact_candidate_log_tracking_table,
        'fact_pic_tracking': transform_fact_pic_tracking_table,
    }

    with TaskGroup("transform_tasks_group") as transform_tasks:
        for name, func in transform_funcs.items():
            PythonOperator(
                task_id=f"transform_{name}",
                python_callable=func
            )

    save_transformed = PythonOperator(
        task_id='save_transformed_tables_to_gcs',
        python_callable=save_dim_and_fact_tables_to_gcs
    )

    create_dwh_dataset = PythonOperator(
        task_id='create_dwh_bigquery_dataset',
        python_callable=create_bigquery_dataset_if_not_exists,
        op_kwargs={'project_id': PROJECT_ID, 'dataset_name': DWH_DATASET}
    )

    with TaskGroup("dwh_load_tasks_group") as dwh_load_tasks:
        for blob_path in list_gcs_avro_files(BUCKET_NAME, prefix="data/dwh"):
            table = Path(blob_path).stem
            GCSToBigQueryOperator(
                task_id=f"load_{table}_to_bq",
                bucket=BUCKET_NAME,
                source_objects=[blob_path],
                destination_project_dataset_table=f"{PROJECT_ID}.{DWH_DATASET}.{table}",
                source_format='AVRO',
                write_disposition='WRITE_TRUNCATE',
                autodetect=True,
                create_disposition='CREATE_IF_NEEDED'
            )

    extract_data >> extract_and_upload >> create_ods_dataset >> ods_load_tasks
    ods_load_tasks >> transform_tasks >> save_transformed >> create_dwh_dataset >> dwh_load_tasks