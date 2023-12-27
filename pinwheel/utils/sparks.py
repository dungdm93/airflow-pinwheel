from pinwheel.kubernetes.models.spark_application import ApplicationStateTypeEnum, SparkApplication


def get_state(spark_app: SparkApplication) -> str | None:
    """Return SparkApp application_state"""
    if not spark_app.status:
        return None
    if not spark_app.status.application_state:
        return None
    return spark_app.status.application_state.state


def get_driver_pod(spark_app: SparkApplication) -> str | None:
    """Return SparkApp driver pod_name"""
    if not spark_app.status:
        return None
    if not spark_app.status.driver_info:
        return None
    return spark_app.status.driver_info.pod_name


def get_error_message(spark_app: SparkApplication) -> str | None:
    """Return SparkApp error_message"""
    if not spark_app.status:
        return None
    if not spark_app.status.application_state:
        return None
    return spark_app.status.application_state.error_message


def is_new(spark_app: SparkApplication) -> bool:
    """Tests if SparkApp is new"""
    state = get_state(spark_app)
    return state is None or state == ApplicationStateTypeEnum.NEW


def is_submitted(spark_app: SparkApplication) -> bool:
    """Tests if SparkApp is submitted"""
    return get_state(spark_app) == ApplicationStateTypeEnum.SUBMITTED


def is_running(spark_app: SparkApplication) -> bool:
    """Tests if SparkApp is running"""
    return get_state(spark_app) == ApplicationStateTypeEnum.RUNNING


def is_finished(spark_app: SparkApplication) -> bool:
    """Tests if SparkApp is finished"""
    return get_state(spark_app) in [
        ApplicationStateTypeEnum.COMPLETED,
        ApplicationStateTypeEnum.FAILED,
    ]


def is_failed(spark_app: SparkApplication) -> bool:
    """Tests if SparkApp is finished and failed"""
    return get_state(spark_app) == ApplicationStateTypeEnum.FAILED


def is_completed(spark_app: SparkApplication) -> bool:
    """Tests if SparkApp is finished and succeeded"""
    return get_state(spark_app) == ApplicationStateTypeEnum.COMPLETED
