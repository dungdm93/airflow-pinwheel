from __future__ import annotations

from typing import TYPE_CHECKING, Any

import kubernetes as k8s
import tenacity
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from kubernetes.client.api_client import ApiClient
from urllib3.exceptions import HTTPError
from urllib3.response import HTTPResponse

if TYPE_CHECKING:
    from kubernetes.client.models import CoreV1EventList, V1Pod


class BaseLauncher(LoggingMixin):
    def __init__(self, k8s_client: ApiClient = None):
        """
        Creates the launcher.

        :param k8s_client: kubernetes API Client
        """
        super().__init__()
        self._core_v1_api = k8s.client.CoreV1Api(k8s_client)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_events(self, k8s_obj: Any) -> CoreV1EventList:
        """Reads events from the k8s Object"""
        selector = [
            f"involvedObject.kind={k8s_obj.kind}",
            f"involvedObject.name={k8s_obj.metadata.name}",
        ]
        if k8s_obj.metadata.uid:
            selector.append(f"involvedObject.uid={k8s_obj.metadata.uid}")

        try:
            return self._core_v1_api.list_namespaced_event(
                namespace=k8s_obj.metadata.namespace,
                field_selector=",".join(selector)
            )
        except HTTPError as e:
            raise AirflowException(
                f"There was an error reading the kubernetes API: {e}"
            )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def delete_events(self, involved_object: Any) -> None:
        """Delete events of given involvedObject"""
        selector = [
            f"involvedObject.kind={involved_object.kind}",
            f"involvedObject.name={involved_object.metadata.name}",
        ]
        if involved_object.metadata.uid:
            selector.append(f"involvedObject.uid={involved_object.metadata.uid}")

        try:
            self._core_v1_api.delete_collection_namespaced_event(
                namespace=involved_object.metadata.namespace,
                field_selector=",".join(selector)
            )
        except HTTPError as e:
            raise AirflowException(
                f"There was an error reading the kubernetes API: {e}"
            )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_pod_logs(self, pod: V1Pod,
                      tail_lines: int = None,
                      timestamps: bool = None,
                      since_seconds: int = None) -> HTTPResponse:
        """Reads log from the POD"""
        additional_kwargs = {}
        if since_seconds:
            additional_kwargs['since_seconds'] = since_seconds
        if timestamps:
            additional_kwargs['timestamps'] = timestamps
        if tail_lines:
            additional_kwargs['tail_lines'] = tail_lines

        try:
            return self._core_v1_api.read_namespaced_pod_log(  # type: ignore
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                follow=True,
                _preload_content=False,
                **additional_kwargs
            )
        except HTTPError as e:
            raise AirflowException(
                f"There was an error reading the kubernetes API: {e}"
            )


__all__ = [
    "BaseLauncher"
]
