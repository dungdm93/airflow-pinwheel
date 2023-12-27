import json
import os

import pytest
from airflow.configuration import conf
from airflow.models import DagBag

curr_dir = os.path.dirname(os.path.realpath(__file__))

secrets_backend_conf = dict(
    connections_file_path=os.path.join(curr_dir, "tests/resources/connections.json"),
    variables_file_path=os.path.join(curr_dir, "tests/resources/variables.json"),
)


@pytest.fixture
def dagbag() -> DagBag:
    conf.set("secrets", "backend", "airflow.secrets.local_filesystem.LocalFilesystemBackend")
    conf.set("secrets", "backend_kwargs", json.dumps(secrets_backend_conf))

    return DagBag(dag_folder=curr_dir, include_examples=False)
