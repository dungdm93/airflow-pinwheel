from airflow.utils.context import Context

from pinwheel.kubernetes.models.spark_application import SparkApplication
from pinwheel.operators.spark_kubernetes import SparkAppMutatingHook
from pinwheel.utils.secrets import get_s3_opts


def add_s3_config(conn_id: str, bucket: str = None) -> SparkAppMutatingHook:
    def func(spark_app: SparkApplication, context: Context) -> None:
        if spark_app.spec.hadoop_conf is None:
            spark_app.spec.hadoop_conf = {}
        hadoop_conf = spark_app.spec.hadoop_conf
        prefix = f"fs.s3a.bucket.{bucket}" if bucket else "fs.s3a"
        s3_opts = get_s3_opts(conn_id)

        hadoop_conf.update({
            f"{prefix}.endpoint": s3_opts.endpoint,
            f"{prefix}.access.key": s3_opts.access_key,
            f"{prefix}.secret.key": s3_opts.secret_key,
            f"{prefix}.path.style.access": "true",
            f"{prefix}.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        })

    return func
