"""Create client-mode Spark sessions from an Orchestera notebook pod."""

# The package dependencies are installed in the project uv environment. Some
# editor hosts do not expose that environment to Pyright.
# pyright: reportMissingImports=false

import json
import logging
import os
import socket
import tempfile
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import yaml
from pyspark.sql import SparkSession

from orchestera.kubernetes.pod_spec_builder import build_executor_pod_spec

logger = logging.getLogger(__name__)


def get_kubernetes_host_addr() -> str:
    """Return the in-cluster Kubernetes API endpoint."""
    host = os.environ.get("KUBERNETES_SERVICE_HOST")
    port = os.environ.get("KUBERNETES_SERVICE_PORT")
    if not host or not port:
        raise ValueError(
            "KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be set"
        )
    return f"https://{host}:{port}"


def _json_object_from_env(name: str) -> Optional[dict[str, str]]:
    value = os.environ.get(name)
    if not value:
        return None
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must be a JSON object") from exc
    if not isinstance(decoded, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in decoded.items()
    ):
        raise ValueError(f"{name} must be a JSON object with string keys and values")
    return decoded


def _json_list_from_env(name: str) -> Optional[list[dict[str, Any]]]:
    value = os.environ.get(name)
    if not value:
        return None
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must be a JSON list") from exc
    if not isinstance(decoded, list) or not all(
        isinstance(item, dict) for item in decoded
    ):
        raise ValueError(f"{name} must be a JSON list of objects")
    return decoded


class OrchesteraSparkSession:
    """A client-mode Spark session whose executor settings come from the notebook pod."""

    def __init__(
        self,
        *,
        app_name: str,
        executor_instances: int,
        executor_cores: int,
        executor_memory: str,
        spark_jars_packages: Optional[str] = None,
        additional_spark_conf: Optional[Mapping[str, str]] = None,
        executor_image: Optional[str] = None,
        service_account_name: Optional[str] = None,
        event_log_dir: Optional[str] = None,
        node_selector: Optional[Mapping[str, str]] = None,
        tolerations: Optional[Sequence[Mapping[str, Any]]] = None,
        python_executable: Optional[str] = None,
    ) -> None:
        self.app_name = app_name
        self.executor_instances = executor_instances
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.spark_jars_packages = spark_jars_packages
        self.additional_spark_conf = dict(additional_spark_conf or {})
        self.executor_image = executor_image or os.environ.get(
            "ORCH_SPARK_K8S_CONTAINER_IMAGE"
        )
        self.service_account_name = service_account_name or os.environ.get(
            "ORCH_SPARK_K8S_SERVICE_ACCOUNT", "workload"
        )
        self.event_log_dir = (
            event_log_dir
            if event_log_dir is not None
            else os.environ.get("ORCH_SPARK_EVENT_LOG_DIR")
        )
        self.node_selector = (
            dict(node_selector)
            if node_selector is not None
            else _json_object_from_env("ORCH_SPARK_K8S_NODE_SELECTOR")
        )
        self.tolerations = (
            list(tolerations)
            if tolerations is not None
            else _json_list_from_env("ORCH_SPARK_K8S_TOLERATIONS")
        )
        self.python_executable = python_executable or os.environ.get(
            "PYSPARK_PYTHON", "/opt/venv/bin/python"
        )
        self.spark: Optional[SparkSession] = None
        self._executor_pod_template_file: Optional[str] = None

    def __enter__(self) -> SparkSession:
        if not self.executor_image:
            raise ValueError(
                "ORCH_SPARK_K8S_CONTAINER_IMAGE must be set to an immutable image digest"
            )

        driver_namespace = os.environ.get("ORCH_SPARK_K8S_NAMESPACE")
        if not driver_namespace:
            raise ValueError(
                "ORCH_SPARK_K8S_NAMESPACE environment variable must be set"
            )

        driver_host = os.environ.get(
            "SPARK_DRIVER_BIND_ADDRESS"
        ) or socket.gethostbyname(socket.gethostname())
        logger.info(
            "Creating client-mode Spark session in namespace %s", driver_namespace
        )

        builder: Any = SparkSession.builder
        builder = (
            builder.appName(self.app_name)
            .master(f"k8s://{get_kubernetes_host_addr()}")
            .config("spark.submit.deployMode", "client")
            .config("spark.kubernetes.container.image", self.executor_image)
            .config("spark.kubernetes.namespace", driver_namespace)
            .config(
                "spark.kubernetes.authenticate.driver.serviceAccountName",
                self.service_account_name,
            )
            .config("spark.driver.host", driver_host)
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.executor.instances", self.executor_instances)
            .config("spark.executor.memory", self.executor_memory)
            .config("spark.executor.cores", self.executor_cores)
            .config(
                "spark.kubernetes.executor.podTemplateFile",
                self._create_executor_pod_template_file(driver_namespace),
            )
        )

        spark_conf = self._default_spark_confs()
        if self.event_log_dir:
            spark_conf.update(
                {
                    "spark.eventLog.enabled": "true",
                    "spark.eventLog.dir": self.event_log_dir,
                }
            )
        spark_conf.update(self.additional_spark_conf)
        for key, value in spark_conf.items():
            builder = builder.config(key, value)
        if self.spark_jars_packages:
            builder = builder.config("spark.jars.packages", self.spark_jars_packages)

        spark = builder.getOrCreate()
        self.spark = spark
        return spark

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if self.spark:
            self.spark.stop()
            self.spark = None
        if self._executor_pod_template_file:
            Path(self._executor_pod_template_file).unlink(missing_ok=True)
            self._executor_pod_template_file = None

    def _create_executor_pod_template_file(self, namespace: str) -> str:
        secrets = os.environ.get("ORCH_SPARK_K8S_ENVS_LIST")
        pod_spec = build_executor_pod_spec(
            application_name=self.app_name,
            in_cluster=True,
            namespace=namespace,
            secrets=secrets.split(",") if secrets else None,
            service_account_name=self.service_account_name,
            node_selector=self.node_selector,
            tolerations=self.tolerations,
            python_executable=self.python_executable,
        )
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".yaml", mode="w"
        ) as tmpfile:
            yaml.safe_dump(pod_spec, tmpfile, default_flow_style=False)
            self._executor_pod_template_file = tmpfile.name
        return self._executor_pod_template_file

    def _default_spark_confs(self) -> dict[str, str]:
        return {
            "spark.default.parallelism": "4",
            "spark.executor.extraClassPath": "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.746.jar",
            "spark.driver.extraJavaOptions": "-Dcom.amazonaws.sdk.ecsFullUriAllowedHosts=169.254.170.23,localhost,127.0.0.1",
            "spark.executor.extraJavaOptions": "-Dcom.amazonaws.sdk.ecsFullUriAllowedHosts=169.254.170.23,localhost,127.0.0.1",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper",
            "spark.executorEnv.AWS_EC2_METADATA_DISABLED": "true",
            "spark.kubernetes.driverEnv.AWS_EC2_METADATA_DISABLED": "true",
            "spark.executorEnv.HOME": "/tmp",
            "spark.executorEnv.PYSPARK_PYTHON": self.python_executable,
        }
