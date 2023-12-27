import re
from abc import ABCMeta
from enum import Enum
from functools import cached_property
from typing import Any

import kubernetes as k8s
from airflow.kubernetes.pod_generator import make_safe_label_value
from airflow.models import BaseOperator
from airflow.utils.context import Context
from kubernetes.client.api_client import ApiClient
from kubernetes.client.models import V1ObjectMeta

from pinwheel.utils.compatible import get_dag_run_conf

MAX_OBJECT_NAME_LEN = 253


class CleanupPolicy(str, Enum):
    ALWAYS = "ALWAYS"
    ON_COMPLETED = "ON_COMPLETED"
    NEVER = "NEVER"


class BaseKubernetesOperator(BaseOperator, metaclass=ABCMeta):
    def __init__(self, *,
                 log_events_on_failure: bool = True,
                 cleanup_policy: CleanupPolicy = CleanupPolicy.ON_COMPLETED,
                 **kwargs: Any,
                 ) -> None:
        super().__init__(**kwargs)

        self.log_events_on_failure = log_events_on_failure
        self.cleanup_policy = cleanup_policy

    @cached_property
    def api_client(self) -> ApiClient:
        """Cached Kubernetes API client"""
        k8s.config.load_incluster_config()
        return k8s.client.ApiClient()

    def get_cleanup_policy(self, context: Context) -> CleanupPolicy:
        policy = self.cleanup_policy
        conf = get_dag_run_conf(context)
        if conf and ('cleanup_policy' in conf):
            policy_str: str = conf['cleanup_policy']
            policy = CleanupPolicy(policy_str.upper())

        return policy

    def add_airflow_managed_labels(self, context: Context, k8s_obj: Any) -> None:
        """
        Add Airflow managed labels to k8s Object. All that labels start with `airflow.apache.org/`
        """
        metadata: V1ObjectMeta = k8s_obj.metadata
        if metadata.labels is None:
            metadata.labels = {}
        labels = self.create_managed_labels(context)
        metadata.labels.update(labels)

    @staticmethod
    def create_managed_labels(context: Context) -> dict[str, str]:
        dag = context['dag']
        labels = {
            'airflow.apache.org/dag_id': dag.dag_id,
            'airflow.apache.org/task_id': context['task'].task_id,
            'airflow.apache.org/run_id': context['run_id'],
            'airflow.apache.org/execution_date': context['ts'],
            'airflow.apache.org/try_number': str(context['ti'].try_number),
        }

        if dag.is_subdag and dag.parent_dag:
            labels['airflow.apache.org/parent_dag_id'] = dag.parent_dag.dag_id

        if 'owner' in dag.default_args:
            labels['airflow.apache.org/owner'] = dag.default_args['owner']

        for key, value in labels.items():
            safe_label = make_safe_label_value(str(value))
            labels[key] = safe_label
        return labels

    def ensure_object_name(self, context: Context, k8s_obj: Any, maxlength: int = MAX_OBJECT_NAME_LEN) -> None:
        """
        Generate Object's name if not exist base on current context (DAG, task, task-instance).
        Name template is `{dag_id}.{task_id}.{execution_date}.{try_number}`.

        Then it sanitize the name follow k8s naming rule
        ref: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-subdomain-names
        """
        kind = k8s_obj.kind
        md = k8s_obj.metadata
        if (md.name is None) and (md.generate_name is None):
            self.log.info("%s name is empty, generated name from current context.", kind)
            md.name = f"{context['dag'].dag_id}.{context['task'].task_id}.{context['ts_nodash']}.{context['ti'].try_number}"

        if md.name:
            md.name = self.sanitize_name(md.name)
            if len(md.name) <= maxlength:
                return
            self.log.warning("%s=%s name > %i chars. Switch to generate_name!", kind, md.name, maxlength)
            md.generate_name, md.name = md.name, None

        md.generate_name = self.sanitize_name(md.generate_name[:maxlength - 6]) + "-"

    @staticmethod
    def sanitize_name(name: str) -> str:
        name = name.lower().replace("_", "-")
        name = re.sub(r"[^\w\.\-]+", "", name)  # Remove all chars except alphanumeric, '-', '.'
        name = re.sub(r"(\.|\-)[\.\-]+", r"\1", name)  # Remove all continuous '-', '.' chars
        name = name.strip(".-")  # Remove all '-', '.' chars at begin and end
        name = name[:MAX_OBJECT_NAME_LEN]  # Max 253 chars
        return name


__all__ = [
    "CleanupPolicy",
    "BaseKubernetesOperator",
]
