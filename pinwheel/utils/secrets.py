from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from urllib.parse import quote

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection, Variable


@lru_cache
def get_variable(key: str, *args: Any, **kwargs: Any) -> Any:
    return Variable.get(key, *args, **kwargs)


@lru_cache
def get_connection(conn_id: str) -> Connection:
    return BaseHook.get_connection(conn_id)


def mysql_jdbc_conn_builder(conn: Connection) -> str:
    """
    https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html
    f"jdbc:mysql://{host}:{port}/{database}"
    """
    if not conn.host:
        raise AssertionError(f"connection {conn.conn_id}: host is required")

    url = f"jdbc:mysql://{conn.host}"
    if conn.port:
        url += f":{conn.port}"
    if conn.schema:  # database
        url += f"/{quote(conn.schema, safe='')}"
    return url


def postgres_jdbc_conn_builder(conn: Connection) -> str:
    """
    https://jdbc.postgresql.org/documentation/head/connect.html
    f"jdbc:postgresql://{host}:{port}/{database}"
    """
    if not conn.host:
        raise AssertionError(f"connection {conn.conn_id}: host is required")

    url = f"jdbc:postgresql://{conn.host}"
    if conn.port:
        url += f":{conn.port}"
    if conn.schema:  # database
        url += f"/{quote(conn.schema, safe='')}"
    return url


def mssql_jdbc_conn_builder(conn: Connection) -> str:
    """
    https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url
    f"jdbc:sqlserver://{host":{port};databaseName={database}"
    """
    if not conn.host:
        raise AssertionError(f"connection {conn.conn_id}: host is required")

    url = f"jdbc:sqlserver://{conn.host}"
    if conn.port:
        url += f":{conn.port}"
    if conn.schema:  # database
        url += f";databaseName={quote(conn.schema, safe='')}"
    return url


def oracle_jdbc_conn_builder(conn: Connection) -> str:
    """
    https://docs.oracle.com/cd/B28359_01/java.111/b31224/urls.htm#BEIDHCBA
    f"jdbc:oracle:thin:@//{host}:{port}/{database}"
    """
    if not conn.host:
        raise AssertionError(f"connection {conn.conn_id}: host is required")

    url = f"jdbc:oracle:thin:@//{conn.host}"
    if conn.port:
        url += f":{conn.port}"
    if conn.schema:  # database
        url += f"/{quote(conn.schema, safe='')}"
    return url


def clickhouse_jdbc_conn_builder(conn: Connection) -> str:
    """
    https://github.com/ClickHouse/clickhouse-jdbc#usage
    f"jdbc:clickhouse://{host}:{port}/{database}"
    """
    if not conn.host:
        raise AssertionError(f"connection {conn.conn_id}: host is required")

    url = f"jdbc:clickhouse://{conn.host}"
    if conn.port:
        url += f":{conn.port}"
    if conn.schema:  # database
        url += f"/{quote(conn.schema, safe='')}"
    return url


JDBC_CONN_BUILDER: dict[str, Callable[[Connection], str]] = dict(
    mysql=mysql_jdbc_conn_builder,
    postgres=postgres_jdbc_conn_builder,
    mssql=mssql_jdbc_conn_builder,
    oracle=oracle_jdbc_conn_builder,
    clickhouse=clickhouse_jdbc_conn_builder,
)

JDBC_DRIVER_CLASS: dict[str, str] = dict(
    mysql="com.mysql.cj.jdbc.Driver",  # for 5.x: com.mysql.jdbc.Driver
    postgres="org.postgresql.Driver",
    mssql="com.microsoft.sqlserver.jdbc.SQLServerDriver",
    oracle="oracle.jdbc.OracleDriver",  # oracle.jdbc.OracleDriver extends oracle.jdbc.driver.OracleDriver
    clickhouse="ru.yandex.clickhouse.ClickHouseDriver",
    # mongo="com.mongodb.spark.sql.DefaultSource",
)

@dataclass(frozen=True)
class DatabaseOptions:
    url: str
    type: str
    host: str
    port: str
    database: str
    username: str
    password: str
    jdbc_url: str
    jdbc_driver: str


def get_db_opts(conn_id: str) -> DatabaseOptions:
    conn: Connection = get_connection(conn_id)

    jdbc_driver = JDBC_DRIVER_CLASS[conn.conn_type]
    jdbc_url = JDBC_CONN_BUILDER[conn.conn_type](conn)

    assert conn.login is not None
    assert conn.password is not None

    return DatabaseOptions(
        url=conn.get_uri(),
        type=conn.conn_type,
        host=conn.host,
        port=conn.port,
        database=conn.schema,
        username=conn.login,
        password=conn.password,
        jdbc_url=jdbc_url,
        jdbc_driver=jdbc_driver,
    )


@dataclass(frozen=True)
class MongoOptions:
    uri: str
    host: str
    port: str
    database: str
    username: str
    password: str


def get_mongo_opts(conn_id: str) -> MongoOptions:
    conn: Connection = get_connection(conn_id)
    return MongoOptions(
        uri=get_mongo_uri(conn),
        host=conn.host,
        port=conn.port,
        database=conn.schema,
        username=conn.login,
        password=conn.password
    )

def get_mongo_uri(conn: Connection) -> str:
    if not conn.host:
        raise AssertionError(f"connection {conn.conn_id}: host is required")

    return "mongodb://{cred}{host}{port}".format(
        cred=f"{conn.login}:{conn.password}@" if conn.login else "",
        host=conn.host,
        port=f":{conn.port}" if conn.port else ""
    )


@dataclass(frozen=True)
class S3Options:
    endpoint: str
    access_key: str
    secret_key: str
    endpoint_with_authority: str
    lookup: str | None
    api: str
    host: str
    port: str


def get_s3_opts(conn_id: str) -> S3Options:
    conn: Connection = get_connection(conn_id)
    if conn.conn_type != "s3":
        raise AirflowException(f"Connection {conn.conn_type} is not S3")

    conn_extra = conn.extra_dejson
    if "host" in conn_extra:
        endpoint: str = conn_extra["host"]
    elif conn.host:
        endpoint = (
            f"http://{conn.host}:{conn.port}" if conn.port else f"http://{conn.host}"
        )
    else:
        # https://docs.aws.amazon.com/vpc/latest/userguide/vpc-endpoints-s3.html
        raise AirflowException(f"S3 Host/endpoint is required for connection {conn_id}")

    assert conn.login is not None
    assert conn.password is not None

    ewa = Connection(uri=endpoint)
    ewa.login = conn.login
    ewa.password = conn.password

    return S3Options(
        endpoint=endpoint,
        access_key=conn.login,
        secret_key=conn.password,
        endpoint_with_authority=ewa.get_uri(),
        lookup=conn_extra.get("lookup"),
        api=conn_extra.get("api", "s3v4"),
        host=conn.host,
        port=conn.port,
    )
