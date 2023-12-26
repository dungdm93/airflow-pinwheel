import json
from collections.abc import Generator
from typing import Any

import kubernetes as k8s
import tenacity
from airflow.exceptions import AirflowException
from kubernetes.client.models import (
    V1DeleteOptions, V1Pod, V1Status,
)
from kubernetes.client.rest import ApiException
from urllib3.exceptions import HTTPError

from pinwheel.kubernetes.base_launcher import BaseLauncher
from pinwheel.utils import pods as pod_utils
from pinwheel.utils.pods import PodPhase


class PodLauncher(BaseLauncher):
    def create_pod(self, pod: V1Pod, **kwargs: Any) -> V1Pod:
        """Create Pod"""
        sanitized_pod = self._core_v1_api.api_client.sanitize_for_serialization(pod)
        json_pod = json.dumps(sanitized_pod, indent=2)

        try:
            self.log.debug('Pod Creation Request: \n%s', json_pod)
            resp = self._core_v1_api.create_namespaced_pod(
                body=sanitized_pod,
                namespace=pod.metadata.namespace,
                **kwargs)
            self.log.debug('Pod Creation Response: \n%s', resp)
            return resp
        except Exception as e:
            self.log.exception('Exception when attempting to create Namespaced Pod: %s', json_pod)
            raise e

    def delete_pod(self, pod: V1Pod, cleanup_events: bool = True) -> None:
        """Deletes POD"""
        try:
            self._core_v1_api.delete_namespaced_pod(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                body=V1DeleteOptions()
            )
            if cleanup_events:
                self.delete_events(pod)
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    def monitor_pod(self, pod: V1Pod) -> None:
        original_pod = pod
        for pod in self._stream_event(original_pod):
            self.log.info("pod=%s state=%s", pod.metadata.name, pod_utils.get_state(pod))

            state = pod_utils.get_state(pod)
            if state in (PodPhase.SUCCEEDED, PodPhase.FAILED):
                break
            if state != PodPhase.RUNNING:
                continue

            # Pod is running
            for line in self.read_pod_logs(pod, tail_lines=10):
                dline = line.decode("utf-8").strip("\r\n")
                self.log.info("pod | %s", dline)

    def _stream_event(self, pod: V1Pod) -> Generator[V1Pod, None, None]:
        while True:
            try:
                watch = k8s.watch.Watch()
                for event in watch.stream(
                    self._core_v1_api.list_namespaced_pod,
                    namespace=pod.metadata.namespace,
                    field_selector=f"metadata.name={pod.metadata.name}",
                    # timeout_seconds=10 * 60,
                ):
                    if event["type"] == "ERROR":
                        self.log.error("Event: type=%s, raw_object=%s", event["type"], event["raw_object"])
                        status: V1Status = event["object"]
                        raise ApiException(status=status.code, reason=f"{status.reason}: {status.message}")
                    elif event["type"] == "DELETED":
                        self.log.warning("Event: type=%s, raw_object=%s", event["type"], event["raw_object"])
                        self.log.warning("pod=%s has gone", pod.metadata.name)
                        return

                    # event["type"] = "ADDED" | "MODIFIED"
                    self.log.debug("Event: type=%s, raw_object=%s", event["type"], event["raw_object"])
                    yield event["object"]
            except ApiException as e:
                if e.status != 410:
                    raise
                self.log.exception(e)
                # https://kubernetes.io/docs/reference/using-api/api-concepts/#the-resourceversion-parameter
                self.log.warning("Kubernetes resourceVersion is too old, let's retry w/ most recent event")

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_pod(self, pod: V1Pod) -> V1Pod:
        """Read POD information"""
        try:
            return self._core_v1_api.read_namespaced_pod(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace
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
    def list_pods(self, namespace: str,
                  label_selector: list[str] = None,
                  field_selector: list[str] = None) -> Generator[V1Pod, None, None]:
        kwargs = {}
        if label_selector:
            kwargs["label_selector"] = ",".join(label_selector)
        if field_selector:
            kwargs["field_selector"] = ",".join(field_selector)
        try:
            yield from self._core_v1_api.list_namespaced_pod(namespace=namespace, **kwargs).items
        except HTTPError as e:
            raise AirflowException(
                f"There was an error reading the kubernetes API: {e}"
            )


__all__ = [
    "PodLauncher"
]
