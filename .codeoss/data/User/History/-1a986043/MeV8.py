import pandas as pd
from sqlalchemy import create_engine
from fastavro.schema import parse_schema
from fastavro import writer
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowFailException
from google.cloud  import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def test_postgres_connection(**kwargs):
    try:
        hook = PostgresHook(postgres_conn_id='postgresql_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        print(f"Connection successful: {result}")
    except Exception as e:
        raise AirflowFailException(f"Database connection failed: {e}")

# Hàm kết nối đến PostgreSQL bằng postgres_hook
def connect_to_postgres_hook():
    try:
        hook = PostgresHook(postgres_conn_id='postgres_conn')
        return hook.get_sqlalchemy_engine()
    except Exception as e:
        raise AirflowFailException(f"Database engine init failed: {e}")

def checking_data_from_postgrest(engine, schema_name):
    """
    Lấy danh sách các bảng trong schema cụ thể.
    """
    try:
        query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
        AND table_type = 'BASE TABLE'
        """
        df = pd.read_sql(query, engine)
        print(df)
        # Trả về DataFrame chứa tên các bảng
        if df.empty:
            print(f"No tables found in schema '{schema_name}'.")
            return None
        else:
            print(f"Found {len(df)} tables in schema '{schema_name}'.")
        return df
    except Exception as e:
        print(f"Error fetching tables: {e}")
        return None

def extract_data_postgres(engine,table_name, schema_name):
    """
    Lấy dữ liệu từ một bảng cụ thể trong schema.
    """
    try:
        query = f'SELECT * FROM {schema_name}."{table_name}"'
        df = pd.read_sql(query, engine)
        print(f"Extracted {len(df)} rows from {schema_name}.{table_name}.")
        return df
    except Exception as e:
        print(f"Error extracting data from {schema_name}.{table_name}: {e}")
        return None

def process_dataframe(df, original_types):
    for col in df.columns:
        if original_types[col] == 'object':
            df[col] = df[col].astype(str).replace({'nan': None, 'None': None, '': None})
        elif original_types[col] == 'datetime64[ns]':
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S')
            df[col] = df[col].where(df[col].notnull(), None)

def get_avro_schema(df: pd.DataFrame, table_name: str) -> dict:
    """
    Sinh schema Avro từ DataFrame dựa trên kiểu dữ liệu pandas.

    Args:
        df (pd.DataFrame): DataFrame đầu vào.
        table_name (str): Tên bảng (sẽ thành tên record trong schema).

    Returns:
        dict: Avro schema đã parse.
    """
    # Ánh xạ kiểu dữ liệu pandas → Avro
    dtype_to_avro = {
        'object': 'string',
        'int64': 'long',
        'float64': 'double',
        'bool': 'boolean',
        'datetime64[ns]': {
            "type": "long",
            "logicalType": "timestamp-micros"
        },
        'category': 'string',
    }

    schema = {
        "name": table_name,
        "type": "record",
        "fields": []
    }

    for col in df.columns:
        pandas_dtype = str(df[col].dtype)
        avro_type = dtype_to_avro.get(pandas_dtype, 'string')  # mặc định là string nếu không khớp

        # Kiểm tra null
        if isinstance(avro_type, dict):  # logicalType (datetime)
            if df[col].isnull().any():
                avro_type = ["null", avro_type]
        else:
            if df[col].isnull().any():
                avro_type = ["null", avro_type]

        # Thêm cột vào schema
        schema['fields'].append({
            "name": col,
            "type": avro_type
        })

    return parse_schema(schema)


# save arvo file local
def save_avro_file(df: pd.DataFrame, schema: dict, table_name: str, output_dir: str = "./output") -> str:
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    today_str = datetime.today().strftime('%Y-%m-%d')
    file_path = os.path.join(output_dir, f"{table_name}_{today_str}.avro")

    df_copy = df.copy()

    # 1. Convert datetime64[ns] → timestamp-micros (INT)
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].apply(
                lambda x: int(x.timestamp() * 1_000_000) if pd.notnull(x) else None
            ).astype("Int64")  # ép kiểu toàn bộ cột thành int64 Nullable


    # 2. Convert NaN → None for all other columns
    records = df_copy.to_dict(orient="records")
    for record in records:
        for key, value in record.items():
            if isinstance(value, float) and pd.isna(value):
                record[key] = None

    # 3. Write Avro
    with open(file_path, "wb") as out:
        writer(out, schema, records)
    # 4. upload arvo to GCS 
    print(f"Avro file saved to: {file_path}")
    return file_path

#automate save all table extracted from postgres to gcs in avro file
from google.cloud import storage
import shutil

def extract_all_and_upload_to_gcs(bucket_name, schema_name="hrms", output_dir="./output", gcs_prefix="data/staging"):
    engine = connect_to_postgres_hook()

    # B1: Lấy danh sách bảng
    table_df = checking_data_from_postgrest(engine, schema_name)
    if table_df is None:
        print("Không có bảng nào để xử lý.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for _, row in table_df.iterrows():
        table_name = row['table_name']
        print(f"Đang xử lý bảng: {table_name}")

        try:
            # B2: Extract
            df = extract_data_postgres(engine, table_name, schema_name)
            if df is None or df.empty:
                print(f"Bảng {table_name} rỗng hoặc lỗi extract.")
                continue

            # B3: Process
            original_types = df.dtypes.astype(str).to_dict()
            process_dataframe(df, original_types)
            schema = get_avro_schema(df, table_name)

            # B4: Save local avro
            avro_path = save_avro_file(df, schema, table_name, output_dir)

            # B5: Upload GCS
            today_str = datetime.today().strftime("%Y%m%d")
            gcs_path = f"{gcs_prefix}/{schema_name}/{table_name}/{table_name}_{today_str}.avro"
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(avro_path)
            print(f"Upload thành công: gs://{bucket_name}/{gcs_path}")

            # B6: Xoá file local
            os.remove(avro_path)

        except Exception as e:
            print(f"Lỗi xử lý bảng {table_name}: {e}")
            continue

    # B7: Xoá thư mục nếu rỗng
    if os.path.exists(output_dir) and not os.listdir(output_dir):
        shutil.rmtree(output_dir)
        print(f"Đã xoá thư mục tạm: {output_dir}")

def create_ods_upload_tasks(bucket_name, project_id, dataset_name, dag, gcs_prefix="data/staging/hrms"):
    gcs_files = list_gcs_avro_files(bucket_name, prefix=gcs_prefix)
    tasks = []
    for blob_path in gcs_files:
        table_name = Path(blob_path).stem
        task = GCSToBigQueryOperator(
            task_id=f'load_ods_{table_name}_to_bigquery',
            bucket=bucket_name,
            source_objects=[blob_path],
            destination_project_dataset_table=f"{project_id}.{dataset_name}.{table_name}",
            source_format='AVRO',
            write_disposition='WRITE_TRUNCATE',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED',
            dag=dag
        )
        tasks.append(task)
    return tasks

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

def save_and_upload_transformed_table(df: pd.DataFrame, table_name: str, bucket_name: str, gcs_prefix: str = "data/dwh", output_dir: str = "./output"):
    """
    Lưu DataFrame dưới dạng Avro và upload lên GCS.
    """
    try:
        original_types = df.dtypes.astype(str).to_dict()
        process_dataframe(df, original_types)
        schema = get_avro_schema(df, table_name)
        avro_path = save_avro_file(df, schema, table_name, output_dir)

        # Tạo đường dẫn GCS
        today_str = datetime.today().strftime("%Y%m%d")
        gcs_path = f"{gcs_prefix}/{table_name}/{table_name}_{today_str}.avro"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(avro_path)
        print(f"Upload thành công: gs://{bucket_name}/{gcs_path}")

        os.remove(avro_path)
    except Exception as e:
        print(f"Lỗi lưu/upload bảng {table_name}: {e}")

def save_dim_and_fact_tables_to_gcs():
    bucket = 'asia-southeast1-datahub-com-9e823cf2-bucket'
    # transform từng bảng rồi lưu + upload
    save_and_upload_transformed_table(transform_dim_job_table(), 'dim_job', bucket)
    save_and_upload_transformed_table(transform_dim_employee_table(), 'dim_employee', bucket)
    save_and_upload_transformed_table(transform_dim_department_table(), 'dim_department', bucket)
    save_and_upload_transformed_table(transform_dim_candidate_table(), 'dim_candidate', bucket)
    save_and_upload_transformed_table(transform_fact_candidate_log_tracking_table(), 'fact_candidate_log', bucket)
    save_and_upload_transformed_table(transform_fact_pic_tracking_table(), 'fact_pic_tracking', bucket)



def create_bigquery_dataset_if_not_exists(project_id, dataset_name, location="asia-southeast1"):
    client = bigquery.Client(project=project_id)
    dataset_id = f"{project_id}.{dataset_name}"

    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} đã tồn tại.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} đã được tạo.")
from google.cloud import storage

def list_gcs_avro_files(bucket_name, prefix="data/dwh"):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs if blob.name.endswith(".avro")]
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from pathlib import Path

def create_upload_tasks(bucket_name, project_id, dataset_name, dag, gcs_prefix="data/dwh"):
    gcs_files = list_gcs_avro_files(bucket_name, prefix=gcs_prefix)
    tasks = []
    for blob_path in gcs_files:
        table_name = Path(blob_path).stem  
        task = GCSToBigQueryOperator(
            task_id=f'load_{table_name}_to_bigquery',
            bucket=bucket_name,
            source_objects=[blob_path],
            destination_project_dataset_table=f"{project_id}.{dataset_name}.{table_name}",
            source_format='AVRO',
            write_disposition='WRITE_TRUNCATE',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED',
            dag=dag
        )
        tasks.append(task)
    return tasks

with DAG(
    dag_id='extract_and_transform_data_from_postgres',
    start_date=datetime(2024, 1, 1),
    schedule_interval= None,
    catchup=False,
    tags=['test', 'sql'],
) as dag:

    test_conn = PythonOperator(
        task_id='check_postgres_conn',
        python_callable=test_postgres_connection,
    )
    
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=checking_data_from_postgrest,
        op_kwargs={
            'engine': connect_to_postgres_hook(),
            'schema_name': 'hrms'
        }
    )
    upload_all_to_staging_gcs = PythonOperator(
        task_id='extract_and_upload_all_tables_to_gcs',
        python_callable=extract_all_and_upload_to_gcs,
        op_kwargs={
            'bucket_name': 'asia-southeast1-datahub-com-9e823cf2-bucket',  # ← chỉnh bucket tại đây
            'gcs_prefix': 'data/staging',
            'schema_name': 'hrms'
        }
    )
    create_ods_dataset_task = PythonOperator(
    task_id='create_ods_bigquery_dataset',
    python_callable=create_bigquery_dataset_if_not_exists,
    op_kwargs={
        'project_id': 'bright-voltage-462902-s0',
        'dataset_name': 'ods_hrms'
    }
    )

    ods_upload_tasks = create_upload_tasks(
        bucket_name='asia-southeast1-datahub-com-9e823cf2-bucket',
        project_id='bright-voltage-462902-s0',
        dataset_name='ods_hrms',
        dag=dag,
        gcs_prefix='data/staging/hrms'
    )

    transform_job = PythonOperator(
        task_id='transform_dim_job_table',
        python_callable=transform_dim_job_table,
    )
    transform_employee = PythonOperator(
        task_id='transform_dim_employee_table',
        python_callable=transform_dim_employee_table,
    )
    transform_department = PythonOperator(
        task_id='transform_dim_department_table',
        python_callable=transform_dim_department_table,
    )
    transform_candidate = PythonOperator(
        task_id='transform_dim_candidate_table',
        python_callable=transform_dim_candidate_table,
    )
    transform_candidate_log = PythonOperator(
        task_id='transform_fact_candidate_log_tracking_table',
        python_callable=transform_fact_candidate_log_tracking_table,
    )
    transform_pic_tracking_table = PythonOperator(
        task_id='transform_fact_pic_tracking_table',
        python_callable=transform_fact_pic_tracking_table
    )

    save_transformed = PythonOperator(
    task_id='save_transformed_tables_to_gcs',
    python_callable=save_dim_and_fact_tables_to_gcs,
    )
    
    create_dataset_task = PythonOperator(
    task_id='create_bigquery_dataset',
    python_callable=create_bigquery_dataset_if_not_exists,
    op_kwargs={
        'project_id': 'bright-voltage-462902-s0',
        'dataset_name': 'recruitment_dwh'
    }
    )

    upload_tasks = create_upload_tasks(
        bucket_name='asia-southeast1-datahub-com-9e823cf2-bucket',
        project_id='bright-voltage-462902-s0',
        dataset_name='recruitment_dwh',
        dag=dag
    )

    test_conn >> extract_data >> upload_all_to_staging_gcs >>create_ods_dataset_task >> create_ods_dataset_task
    for task in ods_upload_tasks:
        create_ods_dataset_task >> task

    [
        transform_job, transform_employee, transform_department,
        transform_candidate, transform_candidate_log, transform_pic_tracking_table
    ] >> save_transformed >> create_dataset_task

    for task in upload_tasks:
        create_dataset_task >> task
