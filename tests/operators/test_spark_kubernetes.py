import copy
from typing import Any
from unittest.mock import (
    MagicMock, Mock, patch,
)

import pytest
from airflow.exceptions import AirflowException

from pinwheel.kubernetes.models.spark_application import ApplicationStateTypeEnum, SparkApplication
from pinwheel.operators.spark_kubernetes import SparkKubernetesOperator
from pinwheel.utils.pods import PodPhase

from .. import utils


def spark_app_result(result: str):
    def func(spark_app: SparkApplication):
        sa = copy.deepcopy(spark_app)
        sa.status.application_state.state = result
        return sa

    return func


class TestSparkKubernetesOperator:
    def setup(self):
        self.hooks: Any = [MagicMock(), MagicMock()]
        self.operator = SparkKubernetesOperator(
            spark_app=utils.get_spark_app(),
            mutating_hooks=self.hooks,
            task_id="TestSparkKubernetesOperator"
        )

    @patch("pinwheel.operators.spark_kubernetes.SparkAppLauncher")
    @patch.object(SparkKubernetesOperator, "api_client", return_value=None)
    def test_execute_successfully(self, k8s_client: Mock, launcher_class: Mock):
        spark_app = utils.get_spark_app()
        context = utils.mock_context()

        launcher = launcher_class.return_value
        launcher.create_spark_app.return_value = spark_app
        launcher.monitor_spark_app.side_effect = spark_app_result(ApplicationStateTypeEnum.COMPLETED)

        self.operator.execute(context)

        for hook in self.hooks:  # type: Mock
            hook.assert_called_once()
        launcher.create_spark_app.assert_called_once()
        launcher.monitor_spark_app.assert_called_once()

    @patch("pinwheel.operators.spark_kubernetes.SparkAppLauncher")
    @patch.object(SparkKubernetesOperator, "api_client", return_value=None)
    def test_execute_monitor_return_null(self, k8s_client: Mock, launcher_class: Mock):
        spark_app = utils.get_spark_app()
        context = utils.mock_context()

        launcher = launcher_class.return_value
        launcher.create_spark_app.return_value = spark_app
        launcher.monitor_spark_app.return_value = None
        launcher.read_spark_app.side_effect = spark_app_result(ApplicationStateTypeEnum.COMPLETED)

        self.operator.execute(context)

        for hook in self.hooks:  # type: Mock
            hook.assert_called_once()
        launcher.create_spark_app.assert_called_once()
        launcher.monitor_spark_app.assert_called_once()
        launcher.read_spark_app.assert_called_once()

    @pytest.mark.parametrize("task_result", [ApplicationStateTypeEnum.COMPLETED, ApplicationStateTypeEnum.FAILED])
    @patch("pinwheel.operators.spark_kubernetes.SparkAppLauncher")
    @patch.object(SparkKubernetesOperator, "api_client", return_value=None)
    def test_execute_log_events(self, k8s_client: Mock, launcher_class: Mock, task_result: str):
        spark_app = utils.get_spark_app()
        context = utils.mock_context()

        launcher = launcher_class.return_value
        launcher.create_spark_app.return_value = spark_app
        launcher.monitor_spark_app.side_effect = spark_app_result(task_result)

        if task_result == ApplicationStateTypeEnum.COMPLETED:
            self.operator.execute(context)
            launcher.read_events.assert_not_called()
        else:
            with pytest.raises(AirflowException):
                self.operator.execute(context)
            launcher.read_events.assert_called_once()

    @pytest.mark.parametrize("task_result", [PodPhase.SUCCEEDED, PodPhase.FAILED])
    @pytest.mark.parametrize("is_cleanup", [True, False])
    @patch("pinwheel.operators.spark_kubernetes.SparkAppLauncher")
    @patch.object(SparkKubernetesOperator, "api_client", return_value=None)
    def test_execute_cleanup(self, k8s_client: Mock, launcher_class: Mock,
                             task_result: str, is_cleanup: bool):
        spark_app = utils.get_spark_app()
        context = utils.mock_context()

        launcher = launcher_class.return_value
        launcher.create_spark_app.return_value = spark_app
        launcher.read_spark_app.side_effect = spark_app_result(task_result)

        with patch.object(self.operator, "is_cleanup", return_value=is_cleanup):
            if task_result == ApplicationStateTypeEnum.COMPLETED:
                self.operator.execute(context)
            else:
                with pytest.raises(AirflowException):
                    self.operator.execute(context)

            if is_cleanup:
                launcher.delete_spark_app.assert_called_once()
            else:
                launcher.delete_spark_app.assert_not_called()
