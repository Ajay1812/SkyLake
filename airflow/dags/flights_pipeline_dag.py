from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from spark.task_failure import task_failure_alert
from main import extract_clean_and_write_parquet, write_iceberg_table

default_args = {
    'owner' : 'nf_01',
    "start_date" : datetime(2026, 8, 4),
    'retries' : 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_failure_alert,
}

dag = DAG(
    dag_id='flight_events',
    description='Handle metadata using iceberg',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

write_parquet = PythonOperator(
    task_id = 'flight_events_clean_write_parquet',
    python_callable=extract_clean_and_write_parquet,
    sla=timedelta(minutes=5),
    dag=dag
)

write_iceberg = PythonOperator(
    task_id = 'flight_events_write_iceberg',
    python_callable=write_iceberg_table,
    sla=timedelta(minutes=5),
    dag=dag
)

write_parquet >> write_iceberg