import pytest
from airflow.configuration import conf
from airflow.utils import db
from airflow.utils.session import create_session
from airflow.www.app import sync_appbuilder_roles
from airflow.www.extensions.init_appbuilder import init_appbuilder
from flask import Flask


@pytest.fixture
def session():
    with create_session() as session:
        yield session
        session.rollback()


@pytest.fixture(autouse=True, scope="session")
def initial_db_init():
    db.resetdb()
    db.bootstrap_dagbag()
    # minimal app to add roles
    flask_app = Flask(__name__)
    flask_app.config["SQLALCHEMY_DATABASE_URI"] = conf.get("database", "SQL_ALCHEMY_CONN")
    init_appbuilder(flask_app)
    sync_appbuilder_roles(flask_app)
