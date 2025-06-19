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

def append_to_dwh():
    append_queries = [
        f"INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.dim_employee` SELECT * FROM `{PROJECT_ID}.{TEMP_DATASET}.temp_employee`",
        f"INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.dim_department` SELECT * FROM `{PROJECT_ID}.{TEMP_DATASET}.temp_department`",
        f"INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.dim_candidate` SELECT * FROM `{PROJECT_ID}.{TEMP_DATASET}.temp_candidates`",
        f"INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.dim_job` SELECT * FROM `{PROJECT_ID}.{TEMP_DATASET}.temp_job_description`",
        f"INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.fact_candidate_log` SELECT * FROM `{PROJECT_ID}.{TEMP_DATASET}.temp_applyfor`",
        f"INSERT INTO `{PROJECT_ID}.{DWH_DATASET}.fact_pic_tracking` SELECT * FROM `{PROJECT_ID}.{TEMP_DATASET}.temp_person_in_charge`"
    ]
    for q in append_queries:
        client.query(q).result()
