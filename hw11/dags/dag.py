import logging
import pendulum
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.yandex.hooks.yandex import YandexCloudBaseHook

from yandexcloud.operations import OperationError


# YANDEX_CONN_ID = 'b1g2m8iabe8rlr1mgevb'

# Данные вашей инфраструктуры
FOLDER_ID = 'b1guei857l5uucjndisk'
SERVICE_ACCOUNT_ID = 'ajeskoiskh0bd37cpf18'
SUBNET_IDS = ["e9b1kob09879183kabn4", "e2lf4eiglagsrhkfnpe0", "fl8olubt2olkdoca4jaq"]
SECURITY_GROUP_IDS = ["enpvd7qu13i62fuqomu8"]
METASTORE_CLUSTER_ID = 'c9qh6j0i0khfbje11qc8'

JOB_NAME = 'job_with_table'
JOB_SCRIPT = 's3a://etl-part2-for-task-and-data/scripts/job_with_table.py'
JOB_ARGS = []
JOB_PROPERTIES = {
    'spark.executor.instances': '1',
    'spark.sql.warehouse.dir': 's3a://etl-part2-for-task-and-data/warehouse',
}


@task
# 1 этап: создание кластера Apache Spark™
def create_cluster(yc_hook, cluster_spec):
    spark_client = yc_hook.sdk.wrappers.Spark()
    try:
        spark_client.create_cluster(cluster_spec)
    except OperationError as job_error:
        cluster_id = job_error.operation_result.meta.cluster_id
        if cluster_id:
            spark_client.delete_cluster(cluster_id=cluster_id)
        raise
    return spark_client.cluster_id


@task
# 2 этап: запуск задания PySpark
def run_spark_job(yc_hook, cluster_id, job_spec):
    spark_client = yc_hook.sdk.wrappers.Spark()
    try:
        job_operation = spark_client.create_pyspark_job(cluster_id=cluster_id, spec=job_spec)
        job_id = job_operation.response.id
        job_info = job_operation.response
    except OperationError as job_error:
        job_id = job_error.operation_result.meta.job_id
        job_info, _ = spark_client.get_job(cluster_id=cluster_id, job_id=job_id)
        raise
    finally:
        job_log = spark_client.get_job_log(cluster_id=cluster_id, job_id=job_id)
        for line in job_log:
            logging.info(line)
        logging.info("Job info: %s", job_info)


@task(trigger_rule="all_done")
# 3 этап: удаление кластера Apache Spark™
def delete_cluster(yc_hook, cluster_id):
    if cluster_id:
        spark_client = yc_hook.sdk.wrappers.Spark()
        spark_client.delete_cluster(cluster_id=cluster_id)


# Настройки DAG
with DAG(
    dag_id="example_spark",
    start_date=pendulum.datetime(2025, 1, 1),
    schedule=None,
):
    # yc_hook = YandexCloudBaseHook(yandex_conn_id=YANDEX_CONN_ID)
    yc_hook = YandexCloudBaseHook()

    cluster_spec = yc_hook.sdk.wrappers.SparkClusterParameters(
        folder_id=FOLDER_ID,
        service_account_id=SERVICE_ACCOUNT_ID,
        subnet_ids=SUBNET_IDS,
        security_group_ids=SECURITY_GROUP_IDS,
        driver_pool_resource_preset="c2-m8",
        driver_pool_size=1,
        executor_pool_resource_preset="c4-m16",
        executor_pool_min_size=1,
        executor_pool_max_size=2,
        metastore_cluster_id=METASTORE_CLUSTER_ID,
    )
    cluster_id = create_cluster(yc_hook, cluster_spec)

    job_spec = yc_hook.sdk.wrappers.PysparkJobParameters(
        name=JOB_NAME,
        main_python_file_uri=JOB_SCRIPT,
        args=JOB_ARGS,
        properties=JOB_PROPERTIES,
    )
    task_job = run_spark_job(yc_hook, cluster_id, job_spec)
    task_delete = delete_cluster(yc_hook, cluster_id)

    task_job >> task_delete
