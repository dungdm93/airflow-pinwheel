import copy
from typing import Any
from unittest.mock import (
    Mock, call, patch,
)

import pytest
from airflow.exceptions import AirflowException
from assertpy import assert_that
from kubernetes.client.exceptions import ApiException
from kubernetes.client.models import V1Status
from kubernetes.watch import Watch
from requests.exceptions import BaseHTTPError

from pinwheel.kubernetes.deserializer import CustomObjectDeserializer
from pinwheel.kubernetes.models.spark_application import (
    CR_GROUP, CR_SPARK_APP_PLURAL, CR_VERSION,
)
from pinwheel.kubernetes.spark_app_launcher import SparkAppLauncher

from .. import utils


class TestSparkAppLauncher:
    def setup(self):
        self.spark_app_launcher = SparkAppLauncher()
        self.k8s_co_api = self.spark_app_launcher._co_api

    def sanitize_k8s_obj(self, k8s_obj: Any):
        sanitized_obj = self.k8s_co_api.api_client.sanitize_for_serialization(k8s_obj)
        return sanitized_obj

    def test_create_spark_app(self):
        spark_app = utils.get_spark_app()

        with patch.object(self.k8s_co_api, "create_namespaced_custom_object",
                          return_value=self.sanitize_k8s_obj(spark_app)) as func:  # type: Mock
            res = self.spark_app_launcher.create_spark_app(spark_app)
            assert_that(res).is_equal_to(spark_app)
            func.assert_called_once()

        with patch.object(self.k8s_co_api, "create_namespaced_custom_object",
                          side_effect=ApiException(status=123)) as func:  # type: Mock
            with pytest.raises(ApiException) as exinfo:  # ExceptionInfo
                self.spark_app_launcher.create_spark_app(spark_app)
            assert_that(exinfo.value.status).is_equal_to(123)
            func.assert_called_once()

    @patch.object(SparkAppLauncher, 'delete_events')
    def test_delete_spark_app(self, _):
        spark_app = utils.get_spark_app()

        with patch.object(self.k8s_co_api, "delete_namespaced_custom_object") as func:  # type: Mock
            self.spark_app_launcher.delete_spark_app(spark_app)
            func.assert_called_once()

        with patch.object(self.k8s_co_api, "delete_namespaced_custom_object",
                          side_effect=ApiException(status=123)) as func:  # type: Mock
            with pytest.raises(ApiException):
                self.spark_app_launcher.delete_spark_app(spark_app)
            func.assert_called_once()

        with patch.object(self.k8s_co_api, "delete_namespaced_custom_object",
                          side_effect=ApiException(status=404)) as func:  # type: Mock
            self.spark_app_launcher.delete_spark_app(spark_app)
            func.assert_called_once()

    @pytest.mark.parametrize("cleanup_events", [True, False])
    @patch.object(SparkAppLauncher, 'delete_events')
    def test_delete_spark_app_with_cleanup_event(self, delete_events: Mock, cleanup_events: bool):
        spark_app = utils.get_spark_app()

        with patch.object(self.k8s_co_api, "delete_namespaced_custom_object") as func:  # type: Mock
            self.spark_app_launcher.delete_spark_app(spark_app, cleanup_events)
            func.assert_called_once()

            if cleanup_events:
                delete_events.assert_called_once_with(spark_app)
            else:
                delete_events.assert_not_called()

    def test_read_spark_app(self):
        spark_app = utils.get_spark_app()
        sanitized_spark_app = self.sanitize_k8s_obj(spark_app)

        with patch.object(self.k8s_co_api, "get_namespaced_custom_object",
                          return_value=sanitized_spark_app) as func:  # type: Mock
            res = self.spark_app_launcher.read_spark_app(spark_app)
            assert_that(res).is_equal_to(spark_app)
            func.assert_called_once_with(
                name=spark_app.metadata.name,
                namespace=spark_app.metadata.namespace,
                group=CR_GROUP,
                version=CR_VERSION,
                plural=CR_SPARK_APP_PLURAL,
            )

        with patch.object(self.k8s_co_api, "get_namespaced_custom_object",
                          side_effect=[BaseHTTPError('Boom'), sanitized_spark_app]) as func:  # type: Mock
            res = self.spark_app_launcher.read_spark_app(spark_app)
            assert_that(res).is_equal_to(spark_app)
            func.assert_has_calls([call(
                name=spark_app.metadata.name,
                namespace=spark_app.metadata.namespace,
                group=CR_GROUP,
                version=CR_VERSION,
                plural=CR_SPARK_APP_PLURAL,
            )] * 2)

        with patch.object(self.k8s_co_api, "get_namespaced_custom_object",
                          side_effect=[BaseHTTPError('Boom')] * 3) as func:  # type: Mock
            with pytest.raises(AirflowException):
                self.spark_app_launcher.read_spark_app(spark_app)
            func.assert_has_calls([call(
                name=spark_app.metadata.name,
                namespace=spark_app.metadata.namespace,
                group=CR_GROUP,
                version=CR_VERSION,
                plural=CR_SPARK_APP_PLURAL,
            )] * 3)

    @patch.object(CustomObjectDeserializer, "deserialize_data", side_effect=lambda d, t: d)
    def test_list_spark_apps(self, _):
        spark_app = utils.get_spark_app()
        spark_app_list = dict(
            api_version="v1",
            kind="List",
            items=[spark_app]
        )
        call_kwargs = dict(
            group=CR_GROUP,
            version=CR_VERSION,
            plural=CR_SPARK_APP_PLURAL,
        )

        with patch.object(self.k8s_co_api, "list_namespaced_custom_object",
                          return_value=spark_app_list) as func:  # type: Mock
            res = self.spark_app_launcher.list_spark_apps(namespace="foobar")

            assert_that(res).is_iterable()
            assert_that(next(res)).is_same_as(spark_app)
            with pytest.raises(StopIteration):
                next(res)  # no more element

            func.assert_called_once_with(namespace="foobar", **call_kwargs)

        with patch.object(self.k8s_co_api, "list_namespaced_custom_object",
                          return_value=spark_app_list) as func:  # type: Mock
            res = self.spark_app_launcher.list_spark_apps(namespace="foobar",
                                                          label_selector=["app=spark", "version=3.0"])
            assert_that(res).is_iterable()
            assert_that(next(res)).is_same_as(spark_app)
            func.assert_called_once_with(namespace="foobar", label_selector="app=spark,version=3.0", **call_kwargs)

        with patch.object(self.k8s_co_api, "list_namespaced_custom_object",
                          return_value=spark_app_list) as func:  # type: Mock
            res = self.spark_app_launcher.list_spark_apps(namespace="foobar", field_selector=["abc=xyz", "foo=bar"])
            assert_that(res).is_iterable()
            assert_that(next(res)).is_same_as(spark_app)
            func.assert_called_once_with(namespace="foobar", field_selector="abc=xyz,foo=bar", **call_kwargs)

        with patch.object(self.k8s_co_api, "list_namespaced_custom_object",
                          return_value=spark_app_list) as func:  # type: Mock
            res = self.spark_app_launcher.list_spark_apps(namespace="foobar",
                                                          label_selector=["app=spark", "version=3.0"],
                                                          field_selector=["abc=xyz", "foo=bar"])
            assert_that(res).is_iterable()
            assert_that(next(res)).is_equal_to(spark_app)
            func.assert_called_once_with(namespace="foobar",
                                         label_selector="app=spark,version=3.0",
                                         field_selector="abc=xyz,foo=bar",
                                         **call_kwargs)

    def test_stream_event_normally(self):
        spark_app = utils.get_spark_app()
        sanitized_spark_app = self.sanitize_k8s_obj(spark_app)

        error = V1Status(
            api_version="v1",
            kind="Status",
            code=410,
        )

        events = iter([
            dict(type="ADDED", raw_object=sanitized_spark_app, object=sanitized_spark_app),
            dict(type="MODIFIED", raw_object=sanitized_spark_app, object=sanitized_spark_app),
            dict(type="ERROR", raw_object=self.sanitize_k8s_obj(error), object=error),
            dict(type="DELETED", raw_object=sanitized_spark_app, object=sanitized_spark_app),
        ])

        def side_effect(*args, **kwargs):
            return events

        with patch.object(Watch, "stream", side_effect=side_effect) as func:  # type: Mock
            count = 0
            for obj in self.spark_app_launcher._stream_event(spark_app):
                assert_that(obj).is_equal_to(spark_app)
                count += 1
            assert_that(count).is_equal_to(2)
            assert_that(func.call_count).is_equal_to(2)

    def test_stream_event_fails(self):
        spark_app = utils.get_spark_app()
        sanitized_spark_app = self.sanitize_k8s_obj(spark_app)

        error = V1Status(
            api_version="v1",
            kind="Status",
            code=123,
        )

        events = iter([
            dict(type="ADDED", raw_object=sanitized_spark_app, object=sanitized_spark_app),
            dict(type="ERROR", raw_object=self.sanitize_k8s_obj(error), object=error),
            dict(type="MODIFIED", raw_object=sanitized_spark_app, object=sanitized_spark_app),
            dict(type="ERROR", raw_object=self.sanitize_k8s_obj(error), object=error),
            dict(type="DELETED", raw_object=sanitized_spark_app, object=sanitized_spark_app),
        ])

        def side_effect(*args, **kwargs):
            return events

        with patch.object(Watch, "stream", side_effect=side_effect) as func:  # type: Mock
            count = 0
            with pytest.raises(ApiException):
                for obj in self.spark_app_launcher._stream_event(spark_app):
                    assert_that(obj).is_equal_to(spark_app)
                    count += 1
            assert_that(count).is_equal_to(1)
            assert_that(func.call_count).is_equal_to(1)

    def test_monitor_spark_app_normally(self):
        spark_app = utils.get_spark_app()

        def stream_event(*args, **kwargs):
            p1 = copy.deepcopy(spark_app)
            p1.status.application_state.state = ""  # New
            yield p1
            p2 = copy.deepcopy(spark_app)
            p2.status.application_state.state = "SUBMITTED"
            yield p2
            p3 = copy.deepcopy(spark_app)
            p3.status.application_state.state = "RUNNING"
            p3.status.driver_info.pod_name = "spark-pi-abcxyz"
            yield p3
            p4 = copy.deepcopy(spark_app)
            p4.status.application_state.state = "SUCCEEDING"
            yield p4
            p5 = copy.deepcopy(spark_app)
            p5.status.application_state.state = "COMPLETED"
            yield p5

        with patch.object(self.spark_app_launcher, "_stream_event",
                          side_effect=stream_event) as stream_event_func, \
            patch.object(self.spark_app_launcher, "monitor_pod") as monitor_pod_func:  # type: Mock
            self.spark_app_launcher.monitor_spark_app(spark_app)
            stream_event_func.assert_called_once()
            monitor_pod_func.assert_called_once()
