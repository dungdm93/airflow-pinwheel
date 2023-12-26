import json
from collections.abc import Generator
from typing import Any

import kubernetes as k8s
import tenacity
from airflow.exceptions import AirflowException
from kubernetes.client.api_client import ApiClient
from kubernetes.client.models import (
    V1DeleteOptions, V1ObjectMeta, V1Pod, V1Status,
)
from kubernetes.client.rest import ApiException
from urllib3.exceptions import HTTPError

import pinwheel.kubernetes.models.spark_application
from pinwheel.kubernetes.base_launcher import BaseLauncher
from pinwheel.kubernetes.deserializer import CustomObjectDeserializer  # type: ignore
from pinwheel.kubernetes.models.spark_application import (
    CR_GROUP, CR_SPARK_APP_PLURAL, CR_VERSION, SparkApplication,
)
from pinwheel.utils import sparks as spark_utils


class SparkAppLauncher(BaseLauncher):
    """Launches Spark Apps"""
    _deserializer = CustomObjectDeserializer(pinwheel.kubernetes.models.spark_application)

    def __init__(self, k8s_client: ApiClient = None):
        """
        Creates the launcher.

        :param k8s_client: kubernetes API Client
        """
        super().__init__(k8s_client)
        self._co_api = k8s.client.CustomObjectsApi(k8s_client)

    def create_spark_app(self, spark_app: SparkApplication, **kwargs: Any) -> SparkApplication:
        """Create SparkApp"""
        sanitized_spark_app = self._co_api.api_client.sanitize_for_serialization(spark_app)
        json_spark_app = json.dumps(sanitized_spark_app, indent=2)

        try:
            self.log.debug("SparkApp Creation Request: \n%s", json_spark_app)
            data = self._co_api.create_namespaced_custom_object(
                group=CR_GROUP,
                version=CR_VERSION,
                namespace=spark_app.metadata.namespace,
                plural=CR_SPARK_APP_PLURAL,
                body=sanitized_spark_app,
                **kwargs
            )
            spark_app = self._deserializer.deserialize_data(data, SparkApplication)
            self.log.debug("SparkApp Creation Response: \n%s", data)
            return spark_app
        except ApiException as e:
            self.log.exception("Exception when attempting to create Namespaced SparkApp: %s", json_spark_app)
            raise e

    def delete_spark_app(self, spark_app: SparkApplication, cleanup_events: bool = True) -> None:
        """Deletes SparkApp"""
        try:
            self._co_api.delete_namespaced_custom_object(
                group=CR_GROUP,
                version=CR_VERSION,
                namespace=spark_app.metadata.namespace,
                plural=CR_SPARK_APP_PLURAL,
                name=spark_app.metadata.name,
                body=V1DeleteOptions()
            )
            if cleanup_events:
                self.delete_events(spark_app)
        except ApiException as e:
            # If the spark_app is already deleted
            if e.status != 404:
                raise

    def monitor_spark_app(self, spark_app: SparkApplication) -> SparkApplication | None:
        prev_state = None
        original_spark_app = spark_app
        for spark_app in self._stream_event(original_spark_app):
            self._log_spark_app_state(spark_app)

            if spark_utils.is_finished(spark_app):
                return spark_app
            if spark_utils.is_new(spark_app):
                continue

            state = spark_utils.get_state(spark_app)
            if prev_state == state:
                continue
            else:
                prev_state = state

            driver_pod_name = spark_utils.get_driver_pod(spark_app)
            driver_pod = V1Pod(
                api_version="v1",
                kind="Pod",
                metadata=V1ObjectMeta(
                    namespace=spark_app.metadata.namespace,
                    name=driver_pod_name
                )
            )

            if spark_utils.is_submitted(spark_app):
                self.log.info("SparkApp is submitted. Waiting for driver_pod=%s running...", driver_pod_name)
                continue

            if spark_utils.is_running(spark_app):
                self.log.info("spark_driver=%s is running...", driver_pod_name)
                self.monitor_pod(driver_pod)
                self.log.info("spark_driver=%s is finished!", driver_pod_name)
                continue

        return None

    def monitor_pod(self, pod: V1Pod) -> None:
        for line in self.read_pod_logs(pod, tail_lines=10):
            dline = line.decode("utf-8").strip("\r\n")
            self.log.info("spark_driver | %s", dline)

    def _stream_event(self, spark_app: SparkApplication) -> Generator[SparkApplication, None, None]:
        while True:
            try:
                watch = k8s.watch.Watch()
                for event in watch.stream(
                    self._co_api.list_namespaced_custom_object,
                    group=CR_GROUP,
                    version=CR_VERSION,
                    namespace=spark_app.metadata.namespace,
                    plural=CR_SPARK_APP_PLURAL,
                    field_selector=f"metadata.name={spark_app.metadata.name}",
                    # timeout_seconds=10 * 60,
                ):
                    if event["type"] == "ERROR":
                        self.log.error("Event: type=%s, raw_object=%s", event["type"], event["raw_object"])
                        status: V1Status = self._deserializer.deserialize_data(event["raw_object"], V1Status)
                        raise ApiException(status=status.code, reason=f"{status.reason}: {status.message}")
                    elif event["type"] == "DELETED":
                        self.log.warning("Event: type=%s, raw_object=%s", event["type"], event["raw_object"])
                        self.log.warning("spark_app=%s has gone", spark_app.metadata.name)
                        return

                    # event["type"] = "ADDED" | "MODIFIED"
                    self.log.debug("Event: type=%s, raw_object=%s", event["type"], event["raw_object"])

                    obj: SparkApplication = self._deserializer.deserialize_data(event["raw_object"], SparkApplication)
                    yield obj
            except ApiException as e:
                if e.status != 410:
                    raise
                # https://kubernetes.io/docs/reference/using-api/api-concepts/#the-resourceversion-parameter
                self.log.warning("Kubernetes ApiException 410 (Gone): %s", e.reason)
                self.log.warning("Let's retry w/ most recent event")

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_spark_app(self, spark_app: SparkApplication) -> SparkApplication:
        """Read SparkApp information"""
        try:
            data = self._co_api.get_namespaced_custom_object(
                group=CR_GROUP,
                version=CR_VERSION,
                namespace=spark_app.metadata.namespace,
                plural=CR_SPARK_APP_PLURAL,
                name=spark_app.metadata.name
            )
            return self._deserializer.deserialize_data(data, SparkApplication)  # type: ignore
        except HTTPError as e:
            raise AirflowException(
                f"There was an error reading the kubernetes API: {e}"
            )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def list_spark_apps(self, namespace: str,
                        label_selector: list[str] = None,
                        field_selector: list[str] = None) -> Generator[SparkApplication, None, None]:
        kwargs = {}
        if label_selector:
            kwargs["label_selector"] = ",".join(label_selector)
        if field_selector:
            kwargs["field_selector"] = ",".join(field_selector)
        try:
            for data in self._co_api.list_namespaced_custom_object(
                group=CR_GROUP,
                version=CR_VERSION,
                namespace=namespace,
                plural=CR_SPARK_APP_PLURAL,
                **kwargs
            )["items"]:
                spark_app = self._deserializer.deserialize_data(data, SparkApplication)  # type: SparkApplication
                yield spark_app
        except HTTPError as e:
            raise AirflowException(
                f"There was an error reading the kubernetes API: {e}"
            )

    def _log_spark_app_state(self, spark_app: SparkApplication) -> None:
        if spark_utils.is_new(spark_app):
            self.log.info("spark_app=%s state=NEW", spark_app.metadata.name)
        elif spark_utils.is_failed(spark_app):
            self.log.error("spark_app=%s state=%s error_message=%s",
                           spark_app.metadata.name,
                           spark_utils.get_state(spark_app),
                           spark_utils.get_error_message(spark_app))
        else:
            self.log.info("spark_app=%s state=%s driver_pod=%s app_id=%s",
                          spark_app.metadata.name,
                          spark_utils.get_state(spark_app),
                          spark_utils.get_driver_pod(spark_app),
                          spark_app.status.spark_application_id)


__all__ = [
    "SparkAppLauncher"
]
