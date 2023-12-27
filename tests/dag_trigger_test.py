from airflow.models import (
    DAG, BaseOperator, DagBag,
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def trigger_dag_exist(dagbag: DagBag, dag_id: str) -> bool:
    return dag_id in dagbag.dags


def test_trigger_dag_exist(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():  # type: str, DAG
        for task_id, task in dag.task_dict.items():  # type: str, BaseOperator
            if not isinstance(task, TriggerDagRunOperator):
                continue
            if not trigger_dag_exist(dagbag, task.trigger_dag_id):
                raise AssertionError(f"{dag_id}/{task_id} trigger {task.trigger_dag_id}, "
                                     f"but it does not exist")
