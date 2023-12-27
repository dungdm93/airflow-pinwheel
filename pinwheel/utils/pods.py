from airflow.exceptions import AirflowException
from kubernetes.client.models import V1Container, V1Pod


class PodPhase:
    PENDING = 'Pending'
    RUNNING = 'Running'
    FAILED = 'Failed'
    SUCCEEDED = 'Succeeded'
    UNKNOWN = 'Unknown'


def get_state(pod: V1Pod) -> str | None:
    """Return Pod phase"""
    if not pod.status:
        return None
    return pod.status.phase  # type: ignore


def get_container(pod: V1Pod, name: str = "main") -> V1Container:
    for c in pod.spec.containers:  # type: V1Container
        if c.name == name:
            return c
    raise AirflowException(f"Container {name} is not exist")
