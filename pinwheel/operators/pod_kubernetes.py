from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults

from pinwheel.kubernetes.pod_launcher import PodLauncher
from pinwheel.operators.base_kubernetes import BaseKubernetesOperator, CleanupPolicy
from pinwheel.utils import pods as pod_utils
from pinwheel.utils.pods import PodPhase

if TYPE_CHECKING:
    from kubernetes.client.models import CoreV1Event, V1Pod

PodMutatingHook = Callable[["V1Pod", Context], None]


class PodKubernetesOperator(BaseKubernetesOperator):
    """
    Execute a task in a Kubernetes Pod
    """

    @apply_defaults
    def __init__(self, *,
                 pod: V1Pod,
                 mutating_hooks: list[PodMutatingHook] = None,
                 **kwargs: Any,
                 ) -> None:
        super().__init__(**kwargs)

        self.pod = pod
        self.mutating_hooks = mutating_hooks
        self.launcher: PodLauncher | None = None

    def execute(self, context: Context) -> None:
        self.launcher = PodLauncher(self.api_client)
        self.ensure_object_name(context, self.pod)
        self.add_airflow_managed_labels(context, self.pod)

        if self.mutating_hooks is not None:
            for hook in self.mutating_hooks:
                hook(self.pod, context)

        try:
            self.pod = self.launcher.create_pod(self.pod)
            self.launcher.monitor_pod(self.pod)

            self.pod = self.launcher.read_pod(self.pod)

            if pod_utils.get_state(self.pod) == PodPhase.SUCCEEDED:
                return

            if self.log_events_on_failure:
                for event in self.launcher.read_events(self.pod).items:  # type: CoreV1Event
                    self.log.error("Pod Event: type=%s reason=%s timestamp=%s count=%s message=%s",
                                   event.type, event.reason, event.first_timestamp, event.count, event.message)
            raise AirflowException(f'Pod returned a failure: {pod_utils.get_state(self.pod)}')
        finally:
            if self.is_cleanup(context, self.pod):
                self.launcher.delete_pod(self.pod)
                self.log.info("pod=%s is deleted", self.pod.metadata.name)

    def is_cleanup(self, context: Context, pod: V1Pod) -> bool:
        policy = self.get_cleanup_policy(context)

        if policy == CleanupPolicy.ON_COMPLETED:
            status = pod_utils.get_state(pod)
            return status == PodPhase.SUCCEEDED  # type: ignore
        else:
            return policy == CleanupPolicy.ALWAYS

    def on_kill(self) -> None:
        if not self.launcher:
            return
        if not self.pod or not self.pod.metadata.name:
            return
        self.log.info("!!!   Received a kill signal. Time to Die   !!!")
        self.launcher.delete_pod(self.pod)
        self.log.info("Pod %s was killed by user", self.pod.metadata.name)


__all__ = [
    "PodMutatingHook",
    "PodKubernetesOperator",
]
