import copy
from unittest.mock import (
    Mock, call, patch,
)

import pytest
from airflow.exceptions import AirflowException
from assertpy import assert_that
from kubernetes.client.exceptions import ApiException
from kubernetes.client.models import V1PodList, V1Status
from kubernetes.watch import Watch
from requests.exceptions import BaseHTTPError

from pinwheel.kubernetes.pod_launcher import PodLauncher

from .. import utils


class TestPodLauncher:
    def setup(self):
        self.pod_launcher = PodLauncher()
        self.k8s_core_v1_api = self.pod_launcher._core_v1_api

    def test_create_pod(self):
        pod = utils.get_pod()

        with patch.object(self.k8s_core_v1_api, "create_namespaced_pod", return_value=pod) as func:  # type: Mock
            res = self.pod_launcher.create_pod(pod)
            assert_that(res).is_equal_to(pod)
            func.assert_called_once()

        with patch.object(self.k8s_core_v1_api, "create_namespaced_pod",
                          side_effect=ApiException(status=123)) as func:  # type: Mock
            with pytest.raises(ApiException) as exinfo:  # ExceptionInfo
                self.pod_launcher.create_pod(pod)
            assert_that(exinfo.value.status).is_equal_to(123)
            func.assert_called_once()

    @patch.object(PodLauncher, 'delete_events')
    def test_delete_pod(self, _):
        pod = utils.get_pod()

        with patch.object(self.k8s_core_v1_api, "delete_namespaced_pod") as func:  # type: Mock
            self.pod_launcher.delete_pod(pod)
            func.assert_called_once()

        with patch.object(self.k8s_core_v1_api, "delete_namespaced_pod",
                          side_effect=ApiException(status=123)) as func:  # type: Mock
            with pytest.raises(ApiException):
                self.pod_launcher.delete_pod(pod)
            func.assert_called_once()

        with patch.object(self.k8s_core_v1_api, "delete_namespaced_pod",
                          side_effect=ApiException(status=404)) as func:  # type: Mock
            self.pod_launcher.delete_pod(pod)
            func.assert_called_once()

    @pytest.mark.parametrize("cleanup_events", [True, False])
    @patch.object(PodLauncher, 'delete_events')
    def test_delete_pod_with_cleanup_event(self, delete_events: Mock, cleanup_events: bool):
        pod = utils.get_pod()

        with patch.object(self.k8s_core_v1_api, "delete_namespaced_pod") as func:  # type: Mock
            self.pod_launcher.delete_pod(pod, cleanup_events)
            func.assert_called_once()

            if cleanup_events:
                delete_events.assert_called_once_with(pod)
            else:
                delete_events.assert_not_called()

    def test_read_pod(self):
        pod = utils.get_pod()

        with patch.object(self.k8s_core_v1_api, "read_namespaced_pod", return_value=pod) as func:  # type: Mock
            res = self.pod_launcher.read_pod(pod)
            assert_that(res).is_equal_to(pod)
            func.assert_called_once_with(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace
            )

        with patch.object(self.k8s_core_v1_api, "read_namespaced_pod",
                          side_effect=[BaseHTTPError('Boom'), pod]) as func:  # type: Mock
            res = self.pod_launcher.read_pod(pod)
            assert_that(res).is_equal_to(pod)
            func.assert_has_calls([call(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace
            )] * 2)

        with patch.object(self.k8s_core_v1_api, "read_namespaced_pod",
                          side_effect=[BaseHTTPError('Boom')] * 3) as func:  # type: Mock
            with pytest.raises(AirflowException):
                self.pod_launcher.read_pod(pod)
            func.assert_has_calls([call(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace
            )] * 3)

    def test_list_pods(self):
        pod = utils.get_pod()
        pod_list = V1PodList(
            api_version="v1",
            kind="List",
            items=[pod]
        )

        with patch.object(self.k8s_core_v1_api, "list_namespaced_pod", return_value=pod_list) as func:  # type: Mock
            res = self.pod_launcher.list_pods(namespace="foobar")

            assert_that(res).is_iterable()
            assert_that(next(res)).is_same_as(pod)
            with pytest.raises(StopIteration):
                next(res)  # no more element

            func.assert_called_once_with(namespace="foobar")

        with patch.object(self.k8s_core_v1_api, "list_namespaced_pod", return_value=pod_list) as func:  # type: Mock
            res = self.pod_launcher.list_pods(namespace="foobar", label_selector=["app=spark", "version=3.0"])
            assert_that(res).is_iterable()
            assert_that(next(res)).is_same_as(pod)
            func.assert_called_once_with(namespace="foobar", label_selector="app=spark,version=3.0")

        with patch.object(self.k8s_core_v1_api, "list_namespaced_pod", return_value=pod_list) as func:  # type: Mock
            res = self.pod_launcher.list_pods(namespace="foobar", field_selector=["abc=xyz", "foo=bar"])
            assert_that(res).is_iterable()
            assert_that(next(res)).is_same_as(pod)
            func.assert_called_once_with(namespace="foobar", field_selector="abc=xyz,foo=bar")

        with patch.object(self.k8s_core_v1_api, "list_namespaced_pod", return_value=pod_list) as func:  # type: Mock
            res = self.pod_launcher.list_pods(namespace="foobar",
                                              label_selector=["app=spark", "version=3.0"],
                                              field_selector=["abc=xyz", "foo=bar"])
            assert_that(res).is_iterable()
            assert_that(next(res)).is_same_as(pod)
            func.assert_called_once_with(namespace="foobar",
                                         label_selector="app=spark,version=3.0",
                                         field_selector="abc=xyz,foo=bar")

    def test_stream_event_normally(self):
        pod = utils.get_pod()

        error = V1Status(
            api_version="v1",
            kind="Status",
            code=410,
        )

        events = iter([
            dict(type="ADDED", raw_object="raw_object", object=pod),
            dict(type="MODIFIED", raw_object="raw_object", object=pod),
            dict(type="ERROR", raw_object=error, object=error),
            dict(type="DELETED", raw_object="raw_object", object=pod),
        ])

        def side_effect(*args, **kwargs):
            return events

        with patch.object(Watch, "stream", side_effect=side_effect) as func:  # type: Mock
            count = 0
            for obj in self.pod_launcher._stream_event(pod):
                assert_that(obj).is_equal_to(pod)
                count += 1
            assert_that(count).is_equal_to(2)
            assert_that(func.call_count).is_equal_to(2)

    def test_stream_event_fails(self):
        pod = utils.get_pod()

        error = V1Status(
            api_version="v1",
            kind="Status",
            code=123,
        )

        events = iter([
            dict(type="ADDED", raw_object="raw_object", object=pod),
            dict(type="ERROR", raw_object=error, object=error),
            dict(type="MODIFIED", raw_object="raw_object", object=pod),
            dict(type="ERROR", raw_object=error, object=error),
            dict(type="DELETED", raw_object="raw_object", object=pod),
        ])

        def side_effect(*args, **kwargs):
            return events

        with patch.object(Watch, "stream", side_effect=side_effect) as func:  # type: Mock
            count = 0
            with pytest.raises(ApiException):
                for obj in self.pod_launcher._stream_event(pod):
                    assert_that(obj).is_equal_to(pod)
                    count += 1
            assert_that(count).is_equal_to(1)
            assert_that(func.call_count).is_equal_to(1)

    def test_monitor_pod(self):
        pod = utils.get_pod()

        def stream_event(*args, **kwargs):
            p1 = copy.deepcopy(pod)
            p1.status.phase = "Pending"
            yield p1
            p2 = copy.deepcopy(pod)
            p2.status.phase = "Running"
            yield p2
            p3 = copy.deepcopy(pod)
            p3.status.phase = "Succeeded"
            yield p3

        def read_pod_logs(*args, **kwargs):
            yield b"first line"
            yield b"second line"
            yield b"third line"

        with patch.object(self.pod_launcher, "_stream_event",
                          side_effect=stream_event) as stream_event_func, \
            patch.object(self.pod_launcher, "read_pod_logs",
                         side_effect=read_pod_logs) as read_pod_logs_func:  # type: Mock
            self.pod_launcher.monitor_pod(pod)
            stream_event_func.assert_called_once()
            read_pod_logs_func.assert_called_once()
