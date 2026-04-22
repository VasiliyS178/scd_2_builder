"""
Calculate SCD-2 table on Python and Trino.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from scd2_loader import SCD2Loader


TRINO_CONN = "trino_connection_name"
TARGET_TABLE = "catalog_name.schema_name.customer_consents_versioned"
SOURCE_TABLE = "catalog_name.schema_name.customer_consents"
KEY_COLUMNS = "customer_id"
EFF_FROM_COLUMN = "dataflow_dttm"
WORK_SCHEMA = "schema_name"

reload_flg = '{{ dag_run.conf.get("reload_flg") or "0" }}'
partition_column = '{{ dag_run.conf.get("partition_column") or "" }}'
custom_dt = '{{ dag_run.conf.get("custom_dt") or ds }}'

with DAG(
    dag_id="scd2_on_trino",
    start_date=datetime(2026, 3, 2),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    params={
        "reload_flg": "0",
        "partition_column": "",
        "custom_dt": ""
    }
) as dag:
    start = EmptyOperator(task_id='start')

    scd2_load = SCD2Loader(
        task_id="scd2_load",
        trg_table=TARGET_TABLE,
        src_table=SOURCE_TABLE,
        key_columns=KEY_COLUMNS,
        eff_from=EFF_FROM_COLUMN,
        wrk_schema=WORK_SCHEMA,
        reload_flg=reload_flg,
        partition_column=partition_column,
        custom_dt=custom_dt,
        conn_id=TRINO_CONN
    )

    end = EmptyOperator(task_id='end')

    start >> scd2_load >> end
