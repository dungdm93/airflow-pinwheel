from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from .base import KubernetesObject

if TYPE_CHECKING:
    from kubernetes.client.models import (
        V1Affinity, V1Container, V1EnvFromSource, V1EnvVar, V1IngressTLS, V1Lifecycle, V1ObjectMeta, V1PodDNSConfig,
        V1PodSecurityContext, V1Toleration, V1Volume, V1VolumeMount,
    )

CR_GROUP = "sparkoperator.k8s.io"
CR_VERSION = "v1beta2"
CR_SPARK_APP_KIND = "SparkApplication"
CR_SPARK_APP_PLURAL = "sparkapplications"
CR_SCHEDULED_SPARK_APP_KIND = "ScheduledSparkApplication"
CR_SCHEDULED_SPARK_APP_PLURAL = "scheduledsparkapplications"


class ScheduledSparkApplication(KubernetesObject):
    openapi_types = {
        'api_version': 'str',
        'kind': 'str',
        'metadata': 'V1ObjectMeta',
        'spec': 'ScheduledSparkApplicationSpec',
        'status': 'ScheduledSparkApplicationStatus'
    }

    attribute_map = {
        'api_version': 'apiVersion',
        'kind': 'kind',
        'metadata': 'metadata',
        'spec': 'spec',
        'status': 'status'
    }

    def __init__(self,
                 api_version: str = None,
                 kind: str = None,
                 metadata: V1ObjectMeta = None,
                 spec: ScheduledSparkApplicationSpec = None,
                 status: ScheduledSparkApplicationStatus = None):
        """ScheduledSparkApplication - a model defined in OpenAPI"""
        self.api_version = api_version
        self.kind = kind
        self.metadata: V1ObjectMeta = metadata or V1ObjectMeta()
        self.spec: ScheduledSparkApplicationSpec = spec or ScheduledSparkApplicationSpec()
        self.status: ScheduledSparkApplicationStatus = status or ScheduledSparkApplicationStatus()


class SparkApplication(KubernetesObject):
    openapi_types = {
        'api_version': 'str',
        'kind': 'str',
        'metadata': 'V1ObjectMeta',
        'spec': 'SparkApplicationSpec',
        'status': 'SparkApplicationStatus'
    }

    attribute_map = {
        'api_version': 'apiVersion',
        'kind': 'kind',
        'metadata': 'metadata',
        'spec': 'spec',
        'status': 'status'
    }

    def __init__(self,
                 api_version: str = None,
                 kind: str = None,
                 metadata: V1ObjectMeta = None,
                 spec: SparkApplicationSpec = None,
                 status: SparkApplicationStatus = None):
        """SparkApplication represents a Spark application running on and using Kubernetes as a cluster manager."""
        self.api_version = api_version
        self.kind = kind
        self.metadata: V1ObjectMeta = metadata or V1ObjectMeta()
        self.spec: SparkApplicationSpec = spec or SparkApplicationSpec()
        self.status: SparkApplicationStatus = status or SparkApplicationStatus()


class ApplicationState(KubernetesObject):
    openapi_types = {
        'state': 'ApplicationStateType',
        'error_message': 'str'
    }

    attribute_map = {
        'state': 'state',
        'error_message': 'errorMessage'
    }

    def __init__(self,
                 state: ApplicationStateType = None,
                 error_message: str = None):
        """ApplicationState tells the current state of the application and an error message in case of failures."""
        self.state = state
        self.error_message = error_message


ApplicationStateType = str  # alias
"""ApplicationStateType represents the type of the current state of an application."""


class ApplicationStateTypeEnum(str, Enum):
    NEW = ""
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SUBMISSION_FAILED = "SUBMISSION_FAILED"
    PENDING_RERUN = "PENDING_RERUN"
    INVALIDATING = "INVALIDATING"
    SUCCEEDING = "SUCCEEDING"
    FAILING = "FAILING"
    UNKNOWN = "UNKNOWN"


class BatchSchedulerConfiguration(KubernetesObject):
    openapi_types = {
        'queue': 'str',
        'priority_class_name': 'str'
    }

    attribute_map = {
        'queue': 'queue',
        'priority_class_name': 'priorityClassName'
    }

    def __init__(self,
                 queue: str = None,
                 priority_class_name: str = None):
        """BatchSchedulerConfiguration used to configure how to batch scheduling Spark Application."""
        self.queue = queue
        self.priority_class_name = priority_class_name


ConcurrencyPolicy = str  # alias


class ConcurrencyPolicyEnum(str, Enum):
    ALLOW = "Allow"
    FORBID = "Forbid"
    REPLACE = "Replace"


class Dependencies(KubernetesObject):
    openapi_types = {
        'jars': 'list[str]',
        'files': 'list[str]',
        'py_files': 'list[str]'
    }

    attribute_map = {
        'jars': 'jars',
        'files': 'files',
        'py_files': 'pyFiles'
    }

    def __init__(self,
                 jars: list[str] = None,
                 files: list[str] = None,
                 py_files: list[str] = None):
        """Dependencies specifies all possible types of dependencies of a Spark application."""
        self.jars = jars
        self.files = files
        self.py_files = py_files


DeployMode = str  # alias
"""DeployMode describes the type of deployment of a Spark application."""


class DeployModeEnum(str, Enum):
    CLUSTER = "cluster"
    CLIENT = "client"
    IN_CLUSTER_CLIENT = "in-cluster-client"


class DriverInfo(KubernetesObject):
    openapi_types = {
        'web_ui_service_name': 'str',
        'web_ui_port': 'int',
        'web_ui_address': 'str',
        'web_ui_ingress_name': 'str',
        'web_ui_ingress_address': 'str',
        'pod_name': 'str'
    }

    attribute_map = {
        'web_ui_service_name': 'webUIServiceName',
        'web_ui_port': 'webUIPort',
        'web_ui_address': 'webUIAddress',
        'web_ui_ingress_name': 'webUIIngressName',
        'web_ui_ingress_address': 'webUIIngressAddress',
        'pod_name': 'podName'
    }

    def __init__(self,
                 web_ui_service_name: str = None,
                 web_ui_port: int = None,
                 web_ui_address: str = None,
                 web_ui_ingress_name: str = None,
                 web_ui_ingress_address: str = None,
                 pod_name: str = None):
        """DriverInfo captures information about the driver."""
        self.web_ui_service_name = web_ui_service_name
        self.web_ui_port = web_ui_port
        self.web_ui_address = web_ui_address
        self.web_ui_ingress_name = web_ui_ingress_name
        self.web_ui_ingress_address = web_ui_ingress_address
        self.pod_name = pod_name


class SparkPodSpec(KubernetesObject):
    openapi_types = {
        'cores': 'int',
        'core_limit': 'str',
        'core_request': 'str',
        'memory': 'str',
        'memory_overhead': 'str',
        'gpu': 'GPUSpec',
        'java_options': 'str',
        'image': 'str',
        'config_maps': 'list[NamePath]',
        'secrets': 'list[SecretInfo]',
        'env': 'list[V1EnvVar]',
        'env_vars': 'dict(str, str)',
        'env_from': 'list[V1EnvFromSource]',
        'env_secret_key_refs': 'dict(str, NameKey)',
        'labels': 'dict(str, str)',
        'annotations': 'dict(str, str)',
        'volume_mounts': 'list[V1VolumeMount]',
        'affinity': 'V1Affinity',
        'tolerations': 'V1Toleration',
        'security_context': 'V1PodSecurityContext',
        'scheduler_name': 'str',
        'sidecars': 'V1Container',
        'init_containers': 'V1Container',
        'host_network': 'bool',
        'node_selector': 'dict(str, str)',
        'dns_config': 'V1PodDNSConfig',
        'termination_grace_period_seconds': 'int',
        'service_account': 'str'
    }

    attribute_map = {
        'cores': 'cores',
        'core_limit': 'coreLimit',
        'core_request': 'coreRequest',
        'memory': 'memory',
        'memory_overhead': 'memoryOverhead',
        'gpu': 'gpu',
        'java_options': 'javaOptions',
        'image': 'image',
        'config_maps': 'configMaps',
        'secrets': 'secrets',
        'env': 'env',
        'env_vars': 'envVars',
        'env_from': 'envFrom',
        'env_secret_key_refs': 'envSecretKeyRefs',
        'labels': 'labels',
        'annotations': 'annotations',
        'volume_mounts': 'volumeMounts',
        'affinity': 'affinity',
        'tolerations': 'tolerations',
        'security_context': 'securityContext',
        'scheduler_name': 'schedulerName',
        'sidecars': 'sidecars',
        'init_containers': 'initContainers',
        'host_network': 'hostNetwork',
        'node_selector': 'nodeSelector',
        'dns_config': 'dnsConfig',
        'termination_grace_period_seconds': 'terminationGracePeriodSeconds',
        'service_account': 'serviceAccount'
    }

    def __init__(self,
                 cores: int = None,
                 core_limit: str = None,
                 core_request: str = None,
                 memory: str = None,
                 memory_overhead: str = None,
                 gpu: GPUSpec = None,
                 java_options: str = None,
                 image: str = None,
                 config_maps: list[NamePath] = None,
                 secrets: list[SecretInfo] = None,
                 env: list[V1EnvVar] = None,
                 env_vars: dict[str, str] = None,
                 env_from: list[V1EnvFromSource] = None,
                 env_secret_key_refs: dict[str, NameKey] = None,
                 labels: dict[str, str] = None,
                 annotations: dict[str, str] = None,
                 volume_mounts: list[V1VolumeMount] = None,
                 affinity: V1Affinity = None,
                 tolerations: V1Toleration = None,
                 security_context: V1PodSecurityContext = None,
                 scheduler_name: str = None,
                 sidecars: V1Container = None,
                 init_containers: V1Container = None,
                 host_network: bool = None,
                 node_selector: dict[str, str] = None,
                 dns_config: V1PodDNSConfig = None,
                 termination_grace_period_seconds: int = None,
                 service_account: str = None):
        """
        SparkPodSpec defines common things that can be customized for a Spark driver or executor pod.
        TODO: investigate if we should use v1.PodSpec and limit what can be set instead.
        """
        self.cores = cores
        self.core_limit = core_limit
        self.core_request = core_request
        self.memory = memory
        self.memory_overhead = memory_overhead
        self.gpu = gpu
        self.java_options = java_options
        self.image = image
        self.config_maps = config_maps
        self.secrets = secrets
        self.env = env
        self.env_vars = env_vars
        self.env_from = env_from
        self.env_secret_key_refs = env_secret_key_refs
        self.labels = labels
        self.annotations = annotations
        self.volume_mounts = volume_mounts
        self.affinity = affinity
        self.tolerations = tolerations
        self.security_context = security_context
        self.scheduler_name = scheduler_name
        self.sidecars = sidecars
        self.init_containers = init_containers
        self.host_network = host_network
        self.node_selector = node_selector
        self.dns_config = dns_config
        self.termination_grace_period_seconds = termination_grace_period_seconds
        self.service_account = service_account


class DriverSpec(SparkPodSpec):
    openapi_types = {
        'pod_name': 'str',
        'lifecycle': 'V1Lifecycle',
        'kubernetes_master': 'str',
        'service_annotations': 'dict(str, str)',
        **SparkPodSpec.openapi_types
    }

    attribute_map = {
        'pod_name': 'podName',
        'lifecycle': 'lifecycle',
        'kubernetes_master': 'kubernetesMaster',
        'service_annotations': 'serviceAnnotations',
        **SparkPodSpec.attribute_map
    }

    def __init__(self,
                 pod_name: str = None,
                 lifecycle: V1Lifecycle = None,
                 kubernetes_master: str = None,
                 service_annotations: dict[str, str] = None,
                 **kwargs: Any):
        """DriverSpec is specification of the driver."""
        super().__init__(**kwargs)

        self.pod_name = pod_name
        self.lifecycle = lifecycle
        self.kubernetes_master = kubernetes_master
        self.service_annotations = service_annotations


DriverState = str  # alias
"""DriverState tells the current state of a spark driver."""


class DriverStateEnum(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class DynamicAllocation(KubernetesObject):
    openapi_types = {
        'enabled': 'bool',
        'initial_executors': 'int',
        'min_executors': 'int',
        'max_executors': 'int',
        'shuffle_tracking_timeout': 'int'
    }

    attribute_map = {
        'enabled': 'enabled',
        'initial_executors': 'initialExecutors',
        'min_executors': 'minExecutors',
        'max_executors': 'maxExecutors',
        'shuffle_tracking_timeout': 'shuffleTrackingTimeout'
    }

    def __init__(self,
                 enabled: bool = None,
                 initial_executors: int = None,
                 min_executors: int = None,
                 max_executors: int = None,
                 shuffle_tracking_timeout: int = None):
        """DynamicAllocation contains configuration options for dynamic allocation."""
        self.enabled = enabled
        self.initial_executors = initial_executors
        self.min_executors = min_executors
        self.max_executors = max_executors
        self.shuffle_tracking_timeout = shuffle_tracking_timeout


class ExecutorSpec(SparkPodSpec):
    openapi_types = {
        'instances': 'int',
        'delete_on_termination': 'bool',
        **SparkPodSpec.openapi_types
    }

    attribute_map = {
        'instances': 'instances',
        'delete_on_termination': 'deleteOnTermination',
        **SparkPodSpec.attribute_map
    }

    def __init__(self,
                 instances: int = None,
                 delete_on_termination: bool = None,
                 **kwargs: Any):
        """ExecutorSpec is specification of the executor."""
        super().__init__(**kwargs)

        self.instances = instances
        self.delete_on_termination = delete_on_termination


ExecutorState = str  # alias
"""ExecutorState tells the current state of an executor."""


class ExecutorStateEnum(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class GPUSpec(KubernetesObject):
    openapi_types = {
        'name': 'str',
        'quantity': 'int'
    }

    attribute_map = {
        'name': 'name',
        'quantity': 'quantity'
    }

    def __init__(self,
                 name: str = None,
                 quantity: int = None):
        """GPUSpec - a model defined in OpenAPI"""
        self.name = name
        self.quantity = quantity


class MonitoringSpec(KubernetesObject):
    openapi_types = {
        'expose_driver_metrics': 'bool',
        'expose_executor_metrics': 'bool',
        'metrics_properties': 'str',
        'metrics_properties_file': 'str',
        'prometheus': 'PrometheusSpec'
    }

    attribute_map = {
        'expose_driver_metrics': 'exposeDriverMetrics',
        'expose_executor_metrics': 'exposeExecutorMetrics',
        'metrics_properties': 'metricsProperties',
        'metrics_properties_file': 'metricsPropertiesFile',
        'prometheus': 'prometheus'
    }

    def __init__(self,
                 expose_driver_metrics: bool = None,
                 expose_executor_metrics: bool = None,
                 metrics_properties: str = None,
                 metrics_properties_file: str = None,
                 prometheus: PrometheusSpec = None):
        """MonitoringSpec defines the monitoring specification."""
        self.expose_driver_metrics = expose_driver_metrics
        self.expose_executor_metrics = expose_executor_metrics
        self.metrics_properties = metrics_properties
        self.metrics_properties_file = metrics_properties_file
        self.prometheus = prometheus


class NameKey(KubernetesObject):
    openapi_types = {
        'name': 'str',
        'key': 'str'
    }

    attribute_map = {
        'name': 'name',
        'key': 'key'
    }

    def __init__(self,
                 name: str = None,
                 key: str = None):
        """NameKey represents the name and key of a SecretKeyRef."""
        self.name = name
        self.key = key


class NamePath(KubernetesObject):
    openapi_types = {
        'name': 'str',
        'path': 'str'
    }

    attribute_map = {
        'name': 'name',
        'path': 'path'
    }

    def __init__(self,
                 name: str = None,
                 path: str = None):
        """NamePath is a pair of a name and a path to which the named objects should be mounted to."""
        self.name = name
        self.path = path


class PrometheusSpec(KubernetesObject):
    openapi_types = {
        'jmx_exporter_jar': 'str',
        'port': 'int',
        'config_file': 'str',
        'configuration': 'str'
    }

    attribute_map = {
        'jmx_exporter_jar': 'jmxExporterJar',
        'port': 'port',
        'config_file': 'configFile',
        'configuration': 'configuration'
    }

    def __init__(self,
                 jmx_exporter_jar: str = None,
                 port: int = None,
                 config_file: str = None,
                 configuration: str = None):
        """PrometheusSpec defines the Prometheus specification when
        Prometheus is to be used for collecting and exposing metrics."""
        self.jmx_exporter_jar = jmx_exporter_jar
        self.port = port
        self.config_file = config_file
        self.configuration = configuration


class RestartPolicy(KubernetesObject):
    openapi_types = {
        'type': 'RestartPolicyType',
        'on_submission_failure_retries': 'int',
        'on_failure_retries': 'int',
        'on_submission_failure_retry_interval': 'int',
        'on_failure_retry_interval': 'int'
    }

    attribute_map = {
        'type': 'type',
        'on_submission_failure_retries': 'onSubmissionFailureRetries',
        'on_failure_retries': 'onFailureRetries',
        'on_submission_failure_retry_interval': 'onSubmissionFailureRetryInterval',
        'on_failure_retry_interval': 'onFailureRetryInterval'
    }

    def __init__(self,
                 type: RestartPolicyType = None,
                 on_submission_failure_retries: int = None,
                 on_failure_retries: int = None,
                 on_submission_failure_retry_interval: int = None,
                 on_failure_retry_interval: int = None):
        """
        RestartPolicy is the policy of if and in which conditions the controller should restart a terminated application.
        This completely defines actions to be taken on any kind of Failures during an application run.
        """
        self.type = type
        self.on_submission_failure_retries = on_submission_failure_retries
        self.on_failure_retries = on_failure_retries
        self.on_submission_failure_retry_interval = on_submission_failure_retry_interval
        self.on_failure_retry_interval = on_failure_retry_interval


RestartPolicyType = str  # alias


class RestartPolicyTypeEnum(str, Enum):
    NEVER = "Never"
    ON_FAILURE = "OnFailure"
    ALWAYS = "Always"


ScheduleState = str  # alias


class ScheduleStateEnum(str, Enum):
    FAILED_VALIDATION = "FailedValidation"
    SCHEDULED = "Scheduled"


class ScheduledSparkApplicationSpec(KubernetesObject):
    openapi_types = {
        'schedule': 'str',
        'template': 'SparkApplicationSpec',
        'suspend': 'bool',
        'concurrency_policy': 'ConcurrencyPolicy',
        'successful_run_history_limit': 'int',
        'failed_run_history_limit': 'int'
    }

    attribute_map = {
        'schedule': 'schedule',
        'template': 'template',
        'suspend': 'suspend',
        'concurrency_policy': 'concurrencyPolicy',
        'successful_run_history_limit': 'successfulRunHistoryLimit',
        'failed_run_history_limit': 'failedRunHistoryLimit'
    }

    def __init__(self,
                 schedule: str = None,
                 template: SparkApplicationSpec = None,
                 suspend: bool = None,
                 concurrency_policy: ConcurrencyPolicy = None,
                 successful_run_history_limit: int = None,
                 failed_run_history_limit: int = None):
        """ScheduledSparkApplicationSpec - a model defined in OpenAPI"""
        self.schedule = schedule
        self.template = template
        self.suspend = suspend
        self.concurrency_policy = concurrency_policy
        self.successful_run_history_limit = successful_run_history_limit
        self.failed_run_history_limit = failed_run_history_limit


class ScheduledSparkApplicationStatus(KubernetesObject):
    openapi_types = {
        'last_run': 'datetime',
        'next_run': 'datetime',
        'last_run_name': 'str',
        'past_successful_run_names': 'list[str]',
        'past_failed_run_names': 'list[str]',
        'schedule_state': 'ScheduleState',
        'reason': 'str'
    }

    attribute_map = {
        'last_run': 'lastRun',
        'next_run': 'nextRun',
        'last_run_name': 'lastRunName',
        'past_successful_run_names': 'pastSuccessfulRunNames',
        'past_failed_run_names': 'pastFailedRunNames',
        'schedule_state': 'scheduleState',
        'reason': 'reason'
    }

    def __init__(self,
                 last_run: datetime = None,
                 next_run: datetime = None,
                 last_run_name: str = None,
                 past_successful_run_names: list[str] = None,
                 past_failed_run_names: list[str] = None,
                 schedule_state: ScheduleState = None,
                 reason: str = None):
        self.last_run = last_run
        self.next_run = next_run
        self.last_run_name = last_run_name
        self.past_successful_run_names = past_successful_run_names
        self.past_failed_run_names = past_failed_run_names
        self.schedule_state = schedule_state
        self.reason = reason


class SecretInfo(KubernetesObject):
    openapi_types = {
        'name': 'str',
        'path': 'str',
        'secret_type': 'SecretType'
    }

    attribute_map = {
        'name': 'name',
        'path': 'path',
        'secret_type': 'secretType'
    }

    def __init__(self,
                 name: str = None,
                 path: str = None,
                 secret_type: SecretType = None):
        """SecretInfo captures information of a secret."""
        self.name = name
        self.path = path
        self.secret_type = secret_type


SecretType = str  # alias
"""SecretType tells the type of a secret."""


class SparkApplicationSpec(KubernetesObject):
    openapi_types = {
        'type': 'SparkApplicationType',
        'spark_version': 'str',
        'mode': 'DeployMode',
        'image': 'str',
        'image_pull_policy': 'str',
        'image_pull_secrets': 'list[str]',
        'main_class': 'str',
        'main_application_file': 'str',
        'arguments': 'list[str]',
        'spark_conf': 'dict(str, str)',
        'hadoop_conf': 'dict(str, str)',
        'spark_config_map': 'str',
        'hadoop_config_map': 'str',
        'volumes': 'list[V1Volume]',
        'driver': 'DriverSpec',
        'executor': 'ExecutorSpec',
        'deps': 'Dependencies',
        'restart_policy': 'RestartPolicy',
        'node_selector': 'dict(str, str)',
        'failure_retries': 'int',
        'retry_interval': 'int',
        'python_version': 'str',
        'memory_overhead_factor': 'str',
        'monitoring': 'MonitoringSpec',
        'time_to_live_seconds': 'int',
        'batch_scheduler': 'str',
        'batch_scheduler_options': 'BatchSchedulerConfiguration',
        'spark_ui_options': 'SparkUIConfiguration',
        'dynamic_allocation': 'DynamicAllocation'
    }

    attribute_map = {
        'type': 'type',
        'spark_version': 'sparkVersion',
        'mode': 'mode',
        'image': 'image',
        'image_pull_policy': 'imagePullPolicy',
        'image_pull_secrets': 'imagePullSecrets',
        'main_class': 'mainClass',
        'main_application_file': 'mainApplicationFile',
        'arguments': 'arguments',
        'spark_conf': 'sparkConf',
        'hadoop_conf': 'hadoopConf',
        'spark_config_map': 'sparkConfigMap',
        'hadoop_config_map': 'hadoopConfigMap',
        'volumes': 'volumes',
        'driver': 'driver',
        'executor': 'executor',
        'deps': 'deps',
        'restart_policy': 'restartPolicy',
        'node_selector': 'nodeSelector',
        'failure_retries': 'failureRetries',
        'retry_interval': 'retryInterval',
        'python_version': 'pythonVersion',
        'memory_overhead_factor': 'memoryOverheadFactor',
        'monitoring': 'monitoring',
        'time_to_live_seconds': 'timeToLiveSeconds',
        'batch_scheduler': 'batchScheduler',
        'batch_scheduler_options': 'batchSchedulerOptions',
        'spark_ui_options': 'sparkUIOptions',
        'dynamic_allocation': 'dynamicAllocation'
    }

    def __init__(self,
                 type: SparkApplicationType = None,
                 spark_version: str = None,
                 mode: DeployMode = None,
                 image: str = None,
                 image_pull_policy: str = None,
                 image_pull_secrets: list[str] = None,
                 main_class: str = None,
                 main_application_file: str = None,
                 arguments: list[str] = None,
                 spark_conf: dict[str, str] = None,
                 hadoop_conf: dict[str, str] = None,
                 spark_config_map: str = None,
                 hadoop_config_map: str = None,
                 volumes: list[V1Volume] = None,
                 driver: DriverSpec = None,
                 executor: ExecutorSpec = None,
                 deps: Dependencies = None,
                 restart_policy: RestartPolicy = None,
                 node_selector: dict[str, str] = None,
                 failure_retries: int = None,
                 retry_interval: int = None,
                 python_version: str = None,
                 memory_overhead_factor: str = None,
                 monitoring: MonitoringSpec = None,
                 time_to_live_seconds: int = None,
                 batch_scheduler: str = None,
                 batch_scheduler_options: BatchSchedulerConfiguration = None,
                 spark_ui_options: SparkUIConfiguration = None,
                 dynamic_allocation: DynamicAllocation = None):
        """
        SparkApplicationSpec describes the specification of a Spark application using Kubernetes as a cluster manager.
        It carries every pieces of information a spark-submit command takes and recognizes.
        """
        self.type = type
        self.spark_version = spark_version
        self.mode = mode
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.image_pull_secrets = image_pull_secrets
        self.main_class = main_class
        self.main_application_file = main_application_file
        self.arguments = arguments
        self.spark_conf = spark_conf
        self.hadoop_conf = hadoop_conf
        self.spark_config_map = spark_config_map
        self.hadoop_config_map = hadoop_config_map
        self.volumes = volumes
        self.driver: DriverSpec = driver or DriverSpec()
        self.executor: ExecutorSpec = executor or ExecutorSpec()
        self.deps = deps
        self.restart_policy = restart_policy
        self.node_selector = node_selector
        self.failure_retries = failure_retries
        self.retry_interval = retry_interval
        self.python_version = python_version
        self.memory_overhead_factor = memory_overhead_factor
        self.monitoring = monitoring
        self.time_to_live_seconds = time_to_live_seconds
        self.batch_scheduler = batch_scheduler
        self.batch_scheduler_options = batch_scheduler_options
        self.spark_ui_options = spark_ui_options
        self.dynamic_allocation = dynamic_allocation


class SparkApplicationStatus(KubernetesObject):
    openapi_types = {
        'spark_application_id': 'str',
        'submission_id': 'str',
        'last_submission_attempt_time': 'datetime',
        'termination_time': 'datetime',
        'driver_info': 'DriverInfo',
        'application_state': 'ApplicationState',
        'executor_state': 'dict(str, ExecutorState)',
        'execution_attempts': 'int',
        'submission_attempts': 'int',
    }

    attribute_map = {
        'spark_application_id': 'sparkApplicationId',
        'submission_id': 'submissionID',
        'last_submission_attempt_time': 'lastSubmissionAttemptTime',
        'termination_time': 'terminationTime',
        'driver_info': 'driverInfo',
        'application_state': 'applicationState',
        'executor_state': 'executorState',
        'execution_attempts': 'executionAttempts',
        'submission_attempts': 'submissionAttempts',
    }

    def __init__(self,
                 spark_application_id: str = None,
                 submission_id: str = None,
                 last_submission_attempt_time: datetime = None,
                 termination_time: datetime = None,
                 driver_info: DriverInfo = None,
                 application_state: ApplicationState = None,
                 executor_state: dict[str, ExecutorState] = None,
                 execution_attempts: int = None,
                 submission_attempts: int = None):
        """SparkApplicationStatus describes the current status of a Spark application."""
        self.spark_application_id = spark_application_id
        self.submission_id = submission_id
        self.last_submission_attempt_time = last_submission_attempt_time
        self.termination_time = termination_time
        self.driver_info = driver_info
        self.application_state = application_state
        self.executor_state = executor_state
        self.execution_attempts = execution_attempts
        self.submission_attempts = submission_attempts


SparkApplicationType = str  # alias
"""SparkApplicationType describes the type of a Spark application."""


class SparkApplicationTypeEnum(str, Enum):
    JAVA = "Java"
    SCALA = "Scala"
    PYTHON = "Python"
    R = "R"


class SparkUIConfiguration(KubernetesObject):
    openapi_types = {
        'service_port': 'int',
        'ingress_annotations': 'dict(str, str)',
        'ingress_tls': 'list[V1IngressTLS]',
    }

    attribute_map = {
        'service_port': 'servicePort',
        'ingress_annotations': 'ingressAnnotations',
        'ingress_tls': 'ingressTLS',
    }

    def __init__(self,
                 service_port: int = None,
                 ingress_annotations: dict[str, str] = None,
                 ingress_tls: list[V1IngressTLS] = None):
        """SparkUIConfiguration is for driver UI specific configuration parameters."""
        self.service_port = service_port
        self.ingress_annotations = ingress_annotations
        self.ingress_tls = ingress_tls
