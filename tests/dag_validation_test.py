from airflow.models import DagBag
from assertpy import assert_that


def test_import_dags(dagbag: DagBag) -> None:
    assert_that(dagbag.import_errors).is_empty()
