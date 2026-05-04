"""
LEVEL_1_DAG - GCS to BigQuery Airflow Pipeline

Purpose:
    This DAG demonstrates a simple real-world data pipeline:

        GCS Bucket CSV files
                ↓
        Airflow DAG orchestration
                ↓
        BigQuery raw tables
                ↓
        SQL transformation
                ↓
        BigQuery insight/final table

Pipeline Flow:
    1. Load employee.csv from GCS into BigQuery raw table emp_raw
    2. Load departments.csv from GCS into BigQuery raw table dep_raw
    3. Join both raw tables using BigQuery SQL and create final enriched table empDep_in

Important Airflow Concepts Used:
    - DAG: Defines the full workflow/pipeline
    - Task: A single unit of work inside the DAG
    - Operator: Defines what each task does
    - Dependency: Defines task execution order
    - Scheduler: Runs the DAG based on schedule_interval
    - Retry: Automatically retries failed tasks based on configuration
"""

# -----------------------------------------------------------------------------
# 1. IMPORTS
# -----------------------------------------------------------------------------
# Imports bring required Airflow classes and Python modules into this file.

import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago

# GCSToBigQueryOperator is used to load files from Google Cloud Storage into BigQuery.
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# BigQueryInsertJobOperator is used to run SQL queries/jobs in BigQuery.
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# -----------------------------------------------------------------------------
# 2. VARIABLES / CONFIGURATION
# -----------------------------------------------------------------------------
# These variables make the DAG easier to maintain.
# Instead of hardcoding project, dataset, table, and bucket names multiple times,
# we define them once and reuse them.

PROJECT_ID = "your-gcp-project-id"
LOCATION = "US"

# BigQuery datasets
DATASET_NAME_1 = "raw_ds"       # raw layer / staging dataset
DATASET_NAME_2 = "insight_ds"   # final / insight dataset

# BigQuery table names
TABLE_NAME_1 = "emp_raw"
TABLE_NAME_2 = "dep_raw"
TABLE_NAME_3 = "empDep_in"

# GCS bucket name where input CSV files are stored
GCS_BUCKET_NAME = "your-gcs-bucket-name"


# -----------------------------------------------------------------------------
# 3. DEFAULT ARGUMENTS
# -----------------------------------------------------------------------------
# default_args controls common DAG/task behavior like owner, start date,
# retry count, retry delay, and email notification settings.

ARGS = {
    "owner": "your_name",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "email": ["your_name@gmail.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# -----------------------------------------------------------------------------
# 4. SQL TRANSFORMATION QUERY
# -----------------------------------------------------------------------------
# This query creates or replaces the final enriched table.
# It joins employee raw data with department raw data and creates a final table.
# This is similar to SQL/PLSQL transformation logic in traditional ETL pipelines.

QUERY = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME_2}.{TABLE_NAME_3}` AS
SELECT
    e.EmployeeID,
    CONCAT(e.FirstName, ' ', e.LastName) AS FullName,
    e.Email,
    e.DepartmentID,
    e.Salary,
    e.JoinDate,
    d.DepartmentName,
    CAST(e.Salary AS INTEGER) * 0.01 AS Tax
FROM
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_1}` AS e
JOIN
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_2}` AS d
ON
    e.DepartmentID = d.DepartmentID
WHERE e.EmployeeID IS NOT NULL
"""


# -----------------------------------------------------------------------------
# 5. DAG DEFINITION
# -----------------------------------------------------------------------------
# DAG represents the full workflow.
# dag_id is the unique name of the DAG in Airflow UI.
# schedule_interval = "0 5 * * *" means this DAG runs daily at 5 AM.
# default_args applies retry and other settings to tasks inside this DAG.

with DAG(
    dag_id="LEVEL_1_DAG",
    schedule_interval="0 5 * * *",
    description="DAG to load data from GCS to BigQuery and create an enriched employee table",
    default_args=ARGS,
    tags=["data_pipeline", "bigquery", "gcs", "airflow"],
) as dag:

    # -------------------------------------------------------------------------
    # 6. TASK 1 - LOAD EMPLOYEE CSV FROM GCS TO BIGQUERY RAW TABLE
    # -------------------------------------------------------------------------
    # This task reads landing/employee.csv from the GCS bucket and loads it into
    # the BigQuery raw table raw_ds.emp_raw.
    # WRITE_TRUNCATE means the table is overwritten every time the DAG runs.

    task1 = GCSToBigQueryOperator(
        task_id="emp_task",
        bucket=GCS_BUCKET_NAME,
        source_objects=["landing/employee.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_1}",
        schema_fields=[
            {"name": "EmployeeID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "Salary", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "JoinDate", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # -------------------------------------------------------------------------
    # 7. TASK 2 - LOAD DEPARTMENT CSV FROM GCS TO BIGQUERY RAW TABLE
    # -------------------------------------------------------------------------
    # This task reads landing/departments.csv from the GCS bucket and loads it
    # into the BigQuery raw table raw_ds.dep_raw.

    task2 = GCSToBigQueryOperator(
        task_id="dep_task",
        bucket=GCS_BUCKET_NAME,
        source_objects=["landing/departments.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_2}",
        schema_fields=[
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "DepartmentName", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # -------------------------------------------------------------------------
    # 8. TASK 3 - RUN BIGQUERY SQL TRANSFORMATION
    # -------------------------------------------------------------------------
    # This task runs the SQL query defined above.
    # It joins emp_raw and dep_raw and creates the final table insight_ds.empDep_in.

    task3 = BigQueryInsertJobOperator(
        task_id="emp_dep_task",
        configuration={
            "query": {
                "query": QUERY,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )

    # -------------------------------------------------------------------------
    # 9. TASK DEPENDENCIES
    # -------------------------------------------------------------------------
    # (task1, task2) >> task3 means:
    #     - task1 and task2 can run in parallel
    #     - task3 will run only after both task1 and task2 complete successfully
    #
    # Flow:
    #     emp_task  ----\
    #                  ---> emp_dep_task
    #     dep_task  ----/

    (task1, task2) >> task3


# -----------------------------------------------------------------------------
# EXPLANATION
# -----------------------------------------------------------------------------
# This DAG loads employee and department CSV files from a GCS bucket into BigQuery
# raw tables using GCSToBigQueryOperator. After both raw tables are loaded, it runs
# a BigQuery SQL transformation using BigQueryInsertJobOperator to join the data
# and create a final enriched employee-department table.
#
# Airflow manages scheduling, task dependencies, retries, and monitoring.
# This is similar to a traditional ETL pipeline where we load data into staging
# tables first, apply transformation logic, and then create final reporting tables.
