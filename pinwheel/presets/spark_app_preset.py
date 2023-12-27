from typing import Any

from airflow.utils.context import Context
from kubernetes.client.models import (
    V1EnvVar, V1ObjectMeta, V1Volume, V1VolumeMount,
)

from pinwheel.kubernetes.models.spark_application import (
    CR_GROUP, CR_SPARK_APP_KIND, CR_VERSION, DeployModeEnum, DriverSpec, ExecutorSpec, SparkApplication,
    SparkApplicationSpec, SparkApplicationTypeEnum,
)
from pinwheel.operators.spark_kubernetes import SparkAppMutatingHook, SparkKubernetesOperator
from pinwheel.utils.secrets import get_s3_opts
from pinwheel.utils.spark_resources import (
    MEDIUM, SMALL, Resource,
)

DEFAULT_IMAGE = "apache/spark:3.5.0-python3"
DEFAULT_SPARK_VERSION = "3.5"

EVENTLOG_BUCKET = "apps-spark"
EVENTLOG_CONN_ID = "spark-eventlog-conn"


def add_event_log_config(spark_app: SparkApplication, context: Context) -> None:
    prefix = f"fs.s3a.bucket.{EVENTLOG_BUCKET}"
    s3_opts = get_s3_opts(EVENTLOG_CONN_ID)

    if spark_app.spec.hadoop_conf is None:
        spark_app.spec.hadoop_conf = {}
    spark_app.spec.hadoop_conf.update({
        f"{prefix}.endpoint": s3_opts.endpoint,
        f"{prefix}.access.key": s3_opts.access_key,
        f"{prefix}.secret.key": s3_opts.secret_key,
        f"{prefix}.path.style.access": "true",
        f"{prefix}.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    })

    if spark_app.spec.spark_conf is None:
        spark_app.spec.spark_conf = {}
    spark_app.spec.spark_conf.update({
        "spark.eventLog.dir": f"s3a://{EVENTLOG_BUCKET}/spark-events/",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.compress": "true",
        # "spark.eventLog.rolling.enabled": "true",
        # "spark.eventLog.rolling.maxFileSize": "128M",
        # "spark.eventLog.logStageExecutorMetrics": "true",
    })


def add_prometheus_metrics_config(spark_app: SparkApplication, context: Context) -> None:
    if spark_app.spec.spark_conf is None:
        spark_app.spec.spark_conf = {}
    spark_app.spec.spark_conf.update({
        'spark.ui.prometheus.enabled': 'true',
        'spark.executor.processTreeMetrics.enabled': 'true',
    })


def add_airflow_metadata_info(spark_app: SparkApplication, context: Context) -> None:
    """ Provide airflow task_instance info to env so that spark app can use them to trace and analysis """

    task_instance = context['ti']
    for s in [spark_app.spec.driver, spark_app.spec.executor]:
        if s.env is None:
            s.env = []

        s.env.extend([
            V1EnvVar(name='AIRFLOW_DAG_ID', value=task_instance.dag_id),
            V1EnvVar(name='AIRFLOW_TASK_ID', value=task_instance.task_id),
            V1EnvVar(name='AIRFLOW_RUN_ID', value=task_instance.run_id),
        ])


def enable_apache_arrow(spark_app: SparkApplication) -> None:
    if spark_app.spec.spark_conf is None:
        spark_app.spec.spark_conf = {}
    spark_app.spec.spark_conf.update({
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
        "spark.sql.execution.arrow.sparkr.enabled": "true",
        "spark.sql.execution.arrow.sparkr.fallback.enabled": "true",
    })


def enable_apache_hive(spark_app: SparkApplication) -> None:
    if spark_app.spec.spark_conf is None:
        spark_app.spec.spark_conf = {}
    spark_app.spec.spark_conf.update({
        "spark.sql.catalogImplementation": "hive",
        "spark.sql.warehouse.dir": "s3a://apps-hive/warehouse/",
        "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.hive.svc:9083",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
    })


def optimize_io(spark_app: SparkApplication) -> None:
    """
    ref:
    * https://spark.apache.org/docs/latest/sql-performance-tuning.html
    """
    if spark_app.spec.spark_conf is None:
        spark_app.spec.spark_conf = {}
    spark_app.spec.spark_conf.setdefault("spark.sql.files.maxRecordsPerFile", str(1_000_000))
    spark_app.spec.spark_conf.update({
        "spark.sql.sources.ignoreDataLocality": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    })


def create_spark_app_on_kubernetes_task(
    main_application_file: str,
    main_class: str = None,
    arguments: list[str] = None,
    env_vars: list[V1EnvVar] = None,
    image: str = DEFAULT_IMAGE,
    spark_app_type: str = SparkApplicationTypeEnum.PYTHON,
    spark_version: str = DEFAULT_SPARK_VERSION,
    python_version: str = "3",
    spark_conf: dict[str, str] = None,
    hadoop_conf: dict[str, str] = None,
    volumes: list[V1Volume] = None,
    volume_mounts: list[V1VolumeMount] = None,
    driver_resource: Resource = SMALL,
    executor_resource: Resource = MEDIUM,
    executor_instances: int = 4,
    time_to_live_seconds: int = 86400,  # 1d
    mutating_hooks: list[SparkAppMutatingHook] = None,
    **kwargs: Any,
) -> SparkKubernetesOperator:
    if spark_app_type == SparkApplicationTypeEnum.PYTHON:
        main_application_file = f"local:///app/warehouse/{main_application_file}"

    spark_app = SparkApplication(
        api_version=f"{CR_GROUP}/{CR_VERSION}",
        kind=CR_SPARK_APP_KIND,
        metadata=V1ObjectMeta(
            namespace="spark"
        ),
        spec=SparkApplicationSpec(
            type=spark_app_type,
            mode=DeployModeEnum.CLUSTER,
            image=image,
            image_pull_policy="IfNotPresent",
            spark_version=spark_version,
            python_version=python_version,
            main_application_file=main_application_file,
            main_class=main_class,
            arguments=arguments,
            spark_conf=spark_conf or {},
            hadoop_conf=hadoop_conf,
            driver=DriverSpec(
                cores=driver_resource.cores,
                core_request=driver_resource.core_request,
                core_limit=driver_resource.core_limit,
                memory=driver_resource.memory,
                memory_overhead=driver_resource.memory_overhead,
                env=env_vars.copy() if env_vars else None,
                volume_mounts=volume_mounts.copy() if volume_mounts else None,
                service_account="spark",
            ),
            executor=ExecutorSpec(
                instances=executor_instances,
                cores=executor_resource.cores,
                core_request=executor_resource.core_request,
                core_limit=executor_resource.core_limit,
                memory=executor_resource.memory,
                memory_overhead=executor_resource.memory_overhead,
                env=env_vars.copy() if env_vars else None,
                volume_mounts=volume_mounts.copy() if volume_mounts else None,
            ),
            volumes=volumes,
            time_to_live_seconds=time_to_live_seconds
        )
    )

    enable_apache_arrow(spark_app)
    enable_apache_hive(spark_app)
    optimize_io(spark_app)

    mutating_hooks = [] if mutating_hooks is None else mutating_hooks.copy()
    mutating_hooks.append(add_event_log_config)
    mutating_hooks.append(add_prometheus_metrics_config)
    mutating_hooks.append(add_airflow_metadata_info)

    return SparkKubernetesOperator(  # type: ignore
        spark_app=spark_app,
        mutating_hooks=mutating_hooks,
        **kwargs
    )
