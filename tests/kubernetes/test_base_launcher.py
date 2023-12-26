from typing import Any
from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from assertpy import assert_that
from requests.exceptions import BaseHTTPError

from pinwheel.kubernetes.base_launcher import BaseLauncher


class TestBaseLauncher:
    def setup(self):
        self.base_launcher = BaseLauncher()
        self.base_launcher._core_v1_api = self.mock_k8s_core_v1_api = mock.Mock()

    @staticmethod
    def get_pod():
        pod = mock.sentinel
        pod.metadata = mock.MagicMock()
        return pod

    # region read_pod_logs

    def test_read_pod_logs_successfully_returns_logs(self):
        pod = self.get_pod()
        self.mock_k8s_core_v1_api.read_namespaced_pod_log.return_value = pod.logs
        logs = self.base_launcher.read_pod_logs(pod)
        assert_that(logs).is_equal_to(pod.logs)

    @pytest.mark.parametrize("params", [
        dict(tail_lines=100),
        dict(timestamps=True),
        dict(since_seconds=2)
    ], ids=[
        "tail_lines",
        "timestamps",
        "since_seconds"
    ])
    def test_read_pod_logs_successfully_with_params(self, params: dict[str, Any]):
        pod = self.get_pod()
        self.mock_k8s_core_v1_api.read_namespaced_pod_log.side_effect = [pod.logs]
        logs = self.base_launcher.read_pod_logs(pod, **params)
        assert_that(logs).is_equal_to(pod.logs)
        self.mock_k8s_core_v1_api.read_namespaced_pod_log.assert_has_calls([
            mock.call(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                follow=True,
                _preload_content=False,
                **params
            ),
        ])

    def test_read_pod_logs_retries_successfully(self):
        pod = self.get_pod()
        self.mock_k8s_core_v1_api.read_namespaced_pod_log.side_effect = [
            BaseHTTPError('Boom'),
            pod.logs,
        ]
        logs = self.base_launcher.read_pod_logs(pod)
        assert_that(logs).is_equal_to(pod.logs)

        self.mock_k8s_core_v1_api.read_namespaced_pod_log.assert_has_calls(
            [mock.call(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                follow=True,
                _preload_content=False
            )] * 2
        )

    def test_read_pod_logs_retries_fails(self):
        pod = self.get_pod()
        self.mock_k8s_core_v1_api.read_namespaced_pod_log.side_effect = [BaseHTTPError('Boom')] * 3
        with pytest.raises(AirflowException):
            self.base_launcher.read_pod_logs(pod)

    # endregion read_pod_logs

    # region delete_events

    def test_delete_events_successfully(self):
        pod = self.get_pod()
        pod.metadata.uid = None
        self.base_launcher.delete_events(pod)
        self.mock_k8s_core_v1_api.delete_collection_namespaced_event.assert_called_once_with(
            namespace=pod.metadata.namespace,
            field_selector=f"involvedObject.kind={pod.kind},involvedObject.name={pod.metadata.name}",
        )

    def test_delete_events_retries_successfully(self):
        pod = self.get_pod()
        pod.metadata.uid = None
        self.mock_k8s_core_v1_api.delete_collection_namespaced_event.side_effect = [
            BaseHTTPError('Boom'),
            mock.sentinel.events,
        ]
        self.base_launcher.delete_events(pod)
        self.mock_k8s_core_v1_api.delete_collection_namespaced_event.assert_has_calls(
            [mock.call(
                namespace=pod.metadata.namespace,
                field_selector=f"involvedObject.kind={pod.kind},involvedObject.name={pod.metadata.name}",
            )] * 2
        )

    def test_delete_events_retries_fails(self):
        pod = self.get_pod()
        self.mock_k8s_core_v1_api.delete_collection_namespaced_event.side_effect = [BaseHTTPError('Boom')] * 3
        with pytest.raises(AirflowException):
            self.base_launcher.delete_events(pod)

    # endregion delete_events

    # region read_events

    def test_read_events_successfully_returns_events(self):
        pod = self.get_pod()
        self.mock_k8s_core_v1_api.list_namespaced_event.return_value = pod.events
        events = self.base_launcher.read_events(pod)
        assert_that(events).is_equal_to(pod.events)

    def test_read_events_retries_successfully(self):
        pod = self.get_pod()
        pod.metadata.uid = None
        self.mock_k8s_core_v1_api.list_namespaced_event.side_effect = [
            BaseHTTPError('Boom'),
            mock.sentinel.events,
        ]
        events = self.base_launcher.read_events(pod)
        assert_that(events).is_equal_to(pod.events)
        self.mock_k8s_core_v1_api.list_namespaced_event.assert_has_calls(
            [mock.call(
                namespace=pod.metadata.namespace,
                field_selector=f"involvedObject.kind={pod.kind},involvedObject.name={pod.metadata.name}",
            )] * 2
        )

    def test_read_events_retries_fails(self):
        pod = self.get_pod()
        self.mock_k8s_core_v1_api.list_namespaced_event.side_effect = [BaseHTTPError('Boom')] * 3
        with pytest.raises(AirflowException):
            self.base_launcher.read_events(pod)

    # endregion read_events
