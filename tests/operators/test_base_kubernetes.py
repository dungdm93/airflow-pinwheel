from unittest.mock import Mock, patch

import pytest
from assertpy import assert_that

from pinwheel.operators.base_kubernetes import BaseKubernetesOperator, CleanupPolicy

from .. import utils


class TestBaseKubernetesOperator:
    def setup(self):
        self.operator = BaseKubernetesOperator(
            task_id="TestBaseKubernetesOperator"
        )

    def test_get_cleanup_policy(self):
        empty_context = utils.mock_context()
        custom_context = utils.mock_context()
        custom_context.update({
            "dag_run": utils.mock_object(conf={"cleanup_policy": "ALWAYS"})
        })
        default_mixin = self.operator
        custom_mixin = BaseKubernetesOperator(
            cleanup_policy=CleanupPolicy.NEVER,
            task_id="TestBaseKubernetesOperator"
        )

        # Default
        policy = default_mixin.get_cleanup_policy(empty_context)
        assert_that(policy).is_equal_to(CleanupPolicy.ON_COMPLETED)

        # Minxin custom
        policy = custom_mixin.get_cleanup_policy(empty_context)
        assert_that(policy).is_equal_to(CleanupPolicy.NEVER)

        # DagRun custom
        policy = default_mixin.get_cleanup_policy(custom_context)
        assert_that(policy).is_equal_to(CleanupPolicy.ALWAYS)

        # Minxin + DagRun custom
        policy = custom_mixin.get_cleanup_policy(custom_context)
        assert_that(policy).is_equal_to(CleanupPolicy.ALWAYS)

    def test_add_airflow_managed_labels(self):
        pod = utils.get_pod()
        context = utils.mock_context()
        labels = dict(foo="bar", abc="xyz")

        with patch.object(self.operator, "create_managed_labels",
                          return_value=labels) as mock_create_labels:  # type: Mock
            self.operator.add_airflow_managed_labels(context, pod)
            assert_that(pod.metadata.labels).contains_entry(
                dict(foo="bar"),
                dict(abc="xyz"),
            )
            mock_create_labels.assert_called_once_with(context)

    def test_create_managed_labels(self):
        context = utils.mock_context()

        labels = self.operator.create_managed_labels(context)
        assert_that(labels).contains_entry(
            {'airflow.apache.org/dag_id': 'dag_id'},
            {'airflow.apache.org/task_id': 'task_id'},
            {'airflow.apache.org/run_id': 'run_id'},
            {'airflow.apache.org/try_number': '3'},
        )

        context.update(dict(
            is_subdag=True,
            dag=utils.mock_object(
                dag_id="dag_id",
                parent_dag=utils.mock_object(
                    dag_id="parent_dag_id"
                )
            ),
        ))
        labels = self.operator.create_managed_labels(context)
        assert_that(labels).contains_entry({'airflow.apache.org/parent_dag_id': 'parent_dag_id'})

    def test_ensure_object_name(self):
        context = utils.mock_context()

        pod = utils.get_pod(name="foobar")
        self.operator.ensure_object_name(context, pod)
        assert_that(pod.metadata.name).is_equal_to("foobar")

        pod = utils.get_pod(name=None)
        self.operator.ensure_object_name(context, pod)
        assert_that(pod.metadata.name).is_equal_to("dag-id.task-id.20200101t000000.3")

        pod = utils.get_pod(name=None, generate_name="foobar")
        self.operator.ensure_object_name(context, pod)
        assert_that(pod.metadata.generate_name).is_equal_to("foobar-")

        pod = utils.get_pod(name="a" * 20)
        self.operator.ensure_object_name(context, pod, maxlength=13)
        assert_that(pod.metadata.generate_name).is_equal_to("a" * (13 - 6) + "-")

        pod = utils.get_pod(name=None, generate_name="a" * 20)
        self.operator.ensure_object_name(context, pod, maxlength=13)
        assert_that(pod.metadata.generate_name).is_equal_to("a" * (13 - 6) + "-")

    @pytest.mark.parametrize("inp, out", [
        ("FooBar", "foobar"),
        ("foo_bar", "foo-bar"),
        ("Pa$$w0rd#", "paw0rd"),
        (".foobar-", "foobar"),
        ("a" * 300, "a" * 253),
    ], ids=[
        "lowercase",
        "underscore",
        "non-accept-chars",
        "prefix-n-postfix",
        "max-chars",
    ])
    def test_sanitize_name(self, inp, out):
        exp = self.operator.sanitize_name(inp)
        assert_that(exp).is_equal_to(out)
