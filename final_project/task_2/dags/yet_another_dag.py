import datetime
from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import DataprocCreatePysparkJobOperator

CLUSTER_ID = "c9q2ntmpojppnm8nf0jn"
BUCKET = 'etl-part2-for-task-and-data'

with DAG(
        "YET_ANOTHER_AIRFLOW_DAG",
        schedule_interval='@hourly',
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:

    spark_task = DataprocCreatePysparkJobOperator(
        task_id='yet_another_pyspark_task',
        cluster_id=CLUSTER_ID,
        main_python_file_uri=f's3a://{BUCKET}/scripts/fill_parquet.py',
    )

    spark_task
