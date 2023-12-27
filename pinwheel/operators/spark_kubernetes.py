from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.kubernetes.pod_generator import MAX_LABEL_LEN
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults

from pinwheel.kubernetes.spark_app_launcher import SparkAppLauncher  # type: ignore
from pinwheel.operators.base_kubernetes import BaseKubernetesOperator, CleanupPolicy
from pinwheel.utils import sparks as spark_utils

if TYPE_CHECKING:
    from kubernetes.client.models import CoreV1Event

    from pinwheel.kubernetes.models.spark_application import SparkApplication

SparkAppMutatingHook = Callable[["SparkApplication", Context], None]


class SparkKubernetesOperator(BaseKubernetesOperator):
    """
    Creates sparkApplication object in kubernetes cluster:
    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication
    """

    ui_color = '#e16a24'

    @apply_defaults
    def __init__(self, *,
                 spark_app: SparkApplication,
                 mutating_hooks: list[SparkAppMutatingHook] = None,
                 **kwargs: Any):
        super().__init__(**kwargs)

        self.spark_app = spark_app
        self.mutating_hooks = mutating_hooks
        self.launcher: SparkAppLauncher | None = None

    def execute(self, context: Context) -> None:
        self.launcher = SparkAppLauncher(self.api_client)
        self.ensure_object_name(context, self.spark_app, maxlength=MAX_LABEL_LEN)
        self.add_airflow_managed_labels(context, self.spark_app)

        if self.mutating_hooks is not None:
            for hook in self.mutating_hooks:
                hook(self.spark_app, context)

        try:
            self.spark_app = self.launcher.create_spark_app(self.spark_app)
            self.spark_app = self.launcher.monitor_spark_app(self.spark_app) or \
                             self.launcher.read_spark_app(self.spark_app)

            if spark_utils.is_completed(self.spark_app):
                return

            if self.log_events_on_failure:
                for event in self.launcher.read_events(self.spark_app).items:  # type: CoreV1Event
                    self.log.error("SparkApp Event: type=%s reason=%s timestamp=%s count=%s message=%s",
                                   event.type, event.reason, event.first_timestamp, event.count, event.message)
            raise AirflowException(f'SparkApp returned a failure: {spark_utils.get_state(self.spark_app)}')
        finally:
            if self.is_cleanup(context, self.spark_app):
                self.launcher.delete_spark_app(self.spark_app)
                self.log.info("spark_app=%s is deleted", self.spark_app.metadata.name)

    def is_cleanup(self, context: Context, spark_app: SparkApplication) -> bool:
        policy = self.get_cleanup_policy(context)

        if policy == CleanupPolicy.ON_COMPLETED:
            return spark_utils.is_completed(spark_app)
        else:
            return policy == CleanupPolicy.ALWAYS

    def on_kill(self) -> None:
        if not self.launcher:
            return
        if not self.spark_app or not self.spark_app.metadata.name:
            return
        self.log.info("!!!   Received a kill signal. Time to Die   !!!")
        self.launcher.delete_spark_app(self.spark_app)
        self.log.info("SparkApp %s was killed by user", self.spark_app.metadata.name)


__all__ = [
    "SparkAppMutatingHook",
    "SparkKubernetesOperator",
]
