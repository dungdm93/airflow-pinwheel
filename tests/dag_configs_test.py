from os import path

from airflow.models import DAG, DagBag
from assertpy import assert_that, soft_assertions

from pinwheel.utils.strings import remove_prefix


def get_dag_relative_folder(dagbag: DagBag, dag: DAG) -> str:
    dag_rel_folder = path.relpath(dag.folder, dagbag.dag_folder)
    return str(dag_rel_folder).strip("./")


def get_dag_relative_filepath(dagbag: DagBag, dag: DAG) -> str:
    dag_rel_folder = path.relpath(dag.filepath, dagbag.dag_folder)
    return str(dag_rel_folder).strip("./")


def test_dag_metadata_naming_convention() -> None:
    from pinwheel.utils import dag_metadata
    from pinwheel.utils.dag_metadata import DagMetadata

    with soft_assertions():
        for k, v in dag_metadata.__dict__.items():
            if isinstance(v, DagMetadata):
                variable_name = k
                dag_id = v.dag_id

                assert_that(variable_name).starts_with(dag_id)


def test_dag_id_naming_convention(dagbag: DagBag) -> None:
    with soft_assertions():
        for dag_id, dag in dagbag.dags.items():  # type: str, DAG
            assert_that(dag_id, "dag_id lowercase").is_lower()

            name_prefix = get_dag_relative_folder(dagbag, dag).replace("/", "_")
            assert_that(dag_id, "dag_id prefix").starts_with(name_prefix)

            expected_filename = remove_prefix(dag_id, name_prefix)
            expected_filename = remove_prefix(expected_filename, "_")
            assert_that(dag.filepath, "DAG filename").is_named(f"{expected_filename}.py")


def test_dag_tags(dagbag: DagBag) -> None:
    with soft_assertions():
        for dag_id, dag in dagbag.dags.items():  # type: str, DAG
            expected_tags = get_dag_relative_folder(dagbag, dag).split("/")[:2]

            assert_that(dag.tags).described_as("dag's tags") \
                .is_iterable() \
                .contains(*expected_tags)
