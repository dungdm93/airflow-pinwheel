import copy
from typing import Any
from unittest.mock import (
    MagicMock, Mock, patch,
)

import pytest
from airflow.exceptions import AirflowException
from kubernetes.client.models import V1Pod

from pinwheel.operators.pod_kubernetes import PodKubernetesOperator
from pinwheel.utils.pods import PodPhase

from .. import utils


def pod_result(result: str):
    def func(pod: V1Pod):
        p = copy.deepcopy(pod)
        p.status.phase = result
        return p

    return func


class TestPodKubernetesOperator:
    def setup(self):
        self.hooks: Any = [MagicMock(), MagicMock()]
        self.operator = PodKubernetesOperator(
            pod=utils.get_pod(),
            mutating_hooks=self.hooks,
            task_id="TestPodKubernetesOperator"
        )

    @patch("pinwheel.operators.pod_kubernetes.PodLauncher")
    @patch.object(PodKubernetesOperator, "api_client", return_value=None)
    def test_execute_successfully(self, k8s_client: Mock, launcher_class: Mock):
        pod = utils.get_pod()
        context = utils.mock_context()

        launcher = launcher_class.return_value
        launcher.create_pod.return_value = pod
        launcher.read_pod.side_effect = pod_result(PodPhase.SUCCEEDED)

        self.operator.execute(context)

        for hook in self.hooks:  # type: Mock
            hook.assert_called_once()
        launcher.create_pod.assert_called_once()
        launcher.monitor_pod.assert_called_once()
        launcher.read_pod.assert_called_once()

    @pytest.mark.parametrize("task_result", [PodPhase.SUCCEEDED, PodPhase.FAILED])
    @patch("pinwheel.operators.pod_kubernetes.PodLauncher")
    @patch.object(PodKubernetesOperator, "api_client", return_value=None)
    def test_execute_log_events(self, k8s_client: Mock, launcher_class: Mock, task_result: str):
        pod = utils.get_pod()
        context = utils.mock_context()

        launcher = launcher_class.return_value
        launcher.create_pod.return_value = pod
        launcher.read_pod.side_effect = pod_result(task_result)

        if task_result == PodPhase.SUCCEEDED:
            self.operator.execute(context)
            launcher.read_events.assert_not_called()
        else:
            with pytest.raises(AirflowException):
                self.operator.execute(context)
            launcher.read_events.assert_called_once()

    @pytest.mark.parametrize("task_result", [PodPhase.SUCCEEDED, PodPhase.FAILED])
    @pytest.mark.parametrize("is_cleanup", [True, False])
    @patch("pinwheel.operators.pod_kubernetes.PodLauncher")
    @patch.object(PodKubernetesOperator, "api_client", return_value=None)
    def test_execute_cleanup(self, k8s_client: Mock, launcher_class: Mock,
                             task_result: str, is_cleanup: bool):
        pod = utils.get_pod()
        context = utils.mock_context()

        launcher = launcher_class.return_value
        launcher.create_pod.return_value = pod
        launcher.read_pod.side_effect = pod_result(task_result)

        with patch.object(self.operator, "is_cleanup", return_value=is_cleanup):
            if task_result == PodPhase.SUCCEEDED:
                self.operator.execute(context)
            else:
                with pytest.raises(AirflowException):
                    self.operator.execute(context)

            if is_cleanup:
                launcher.delete_pod.assert_called_once()
            else:
                launcher.delete_pod.assert_not_called()
