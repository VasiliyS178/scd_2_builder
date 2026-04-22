"""
Calculate SCD-2 table from slices on Spark 2.4.4 or later.
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from k8s.executors import spark_executor
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from spark import conf_2_4_4


DAG_ID = "scd2_on_spark"
OWNER = ""
AIRFLOW_HOME = os.path.dirname(os.path.abspath(os.path.join(__file__, "../..")))
APP_PATH = f'{AIRFLOW_HOME}/scripts/scd_tables_builder/builder.py'
SETTINGS_PATH = f'{AIRFLOW_HOME}/scripts/scd_tables_builder/settings.py'
MAIL_LIST = []

# set table's names to transform to SCD-2 tables
target_tables = [
    'schemа_example.table_example_hist',
    'schemа_example.table_example_2_hist'
]

spark_config = {
    **conf_2_4_4,
    "spark.dynamicAllocation.maxExecutors": "20",
    "spark.executor.memory": "12g",
    "spark.executor.cores": "4",
    "spark.executor.memoryOverhead": "2g",
    "spark.driver.memory": "8g"
}

default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    "email": MAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=10),
    "retries": 1
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    tags=["scd", "hive"],
    schedule_interval="0 21 * * *",
    start_date=datetime(2024, 11, 25),
    catchup=False,
    max_active_runs=1,
    concurrency=3,
    doc_md=__doc__
)

with dag:
    start = DummyOperator(task_id="START")
    complete = DummyOperator(task_id="COMPLETE")
    for table in target_tables:
        schema = table.split(".")[0]
        table_name = table.split(".")[1]
        with TaskGroup(group_id=schema) as schema_gr:
            append_changes = SparkSubmitOperator(
                task_id=table_name,
                name=table_name,
                application=APP_PATH,
                application_args=[table, "{{ ds }}"],
                conf=spark_config,
                py_files=SETTINGS_PATH,
                conn_id="spark_default",
                executor_config=spark_executor(version_2_4_4=True)
            )
        start >> schema_gr >> complete
