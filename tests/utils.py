from unittest.mock import MagicMock, Mock

from airflow.utils.context import Context
from kubernetes.client.models import *

from pinwheel.kubernetes.models.spark_application import *


def mock_object(**kwargs: Any) -> Mock:
    obj = MagicMock()
    for key, value in kwargs.items():
        setattr(obj, key, value)
    return obj


def mock_context() -> Context:
    context = dict(
        dag=mock_object(dag_id="dag_id"),
        task=mock_object(task_id="task_id"),
        run_id="run_id",
        ts="2020-01-01T00:00:00+00:00",
        ts_nodash="20200101T000000",
        ti=mock_object(try_number=3),
        is_subdag=False,
        dag_run=mock_object(conf={})
    )
    return context  # type: ignore


def get_pod(name: str | None = "foobar", generate_name: str | None = None) -> V1Pod:
    pod = V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=V1ObjectMeta(
            namespace="default",
            name=name,
            generate_name=generate_name
        ),
        spec=V1PodSpec(
            containers=[V1Container(
                name="base",
                image="ubuntu:20.04",
                args=["echo", "Hello world"]
            )]
        ),
        status=V1PodStatus()
    )
    return pod


def get_spark_app(name: str | None = "spark-pi", generate_name: str | None = None):
    spark_app = SparkApplication(
        api_version=f"{CR_GROUP}/{CR_VERSION}",
        kind=CR_SPARK_APP_KIND,
        metadata=V1ObjectMeta(
            namespace="spark",
            name=name,
            generate_name=generate_name
        ),
        spec=SparkApplicationSpec(
            type="Python",
            mode="cluster",
            image="apache/spark:3.5.0-python3",
            image_pull_policy="IfNotPresent",
            spark_version="3.0.0",
            python_version="3",
            main_application_file=f"local:///opt/spark/example/spark-pi.py",
            arguments=[],
            spark_conf={},
            hadoop_conf={},
            driver=DriverSpec(),
            executor=ExecutorSpec(instances=1)
        ),
        status=SparkApplicationStatus(
            application_state=ApplicationState(),
            driver_info=DriverInfo()
        )
    )
    return spark_app
