import logging
import os
import socket
import tempfile

import yaml
from kubernetes import config as kubernetes_config
from pyspark.sql import SparkSession

from orchestera.kubernetes.pod_spec_builder import build_executor_pod_spec

logger = logging.getLogger(__name__)

EXECUTOR_IMAGE = "853027285987.dkr.ecr.us-east-1.amazonaws.com/hello-world:latest"


def get_kubernetes_host_addr():
    """Get kubernetes host adddress"""
    k8s_host = os.environ.get("KUBERNETES_SERVICE_HOST")
    k8s_port = os.environ.get("KUBERNETES_SERVICE_PORT")
    return f"https://{k8s_host}:{k8s_port}"


class OrchesteraSparkSession:
    def __init__(
        self,
        *,
        app_name,
        executor_instances,
        executor_cores,
        executor_memory,
        spark_jars_packages=None,
        additional_spark_conf=None,
    ) -> None:
        self.app_name = app_name
        self.executor_instances = executor_instances
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.spark_jars_packages = spark_jars_packages
        self.additional_spark_conf = additional_spark_conf

    def __enter__(self):
        logging.info("Loading in-cluster config")

        kubernetes_config.load_incluster_config()

        logger.info("Creating spark session with the context manager")

        master_url = f"k8s://{get_kubernetes_host_addr()}"
        driver_host = socket.gethostbyname(socket.gethostname())
        driver_namespace = os.environ.get("ORCH_SPARK_K8S_NAMESPACE")

        if not driver_namespace:
            raise ValueError(
                "ORCH_SPARK_K8S_NAMESPACE environment variable must be set"
            )

        logger.info("Master url is set to %s", master_url)
        logger.info("spark.driver.host is set to %s", driver_host)

        builder = (
            SparkSession.builder.appName("SparkK8sApp")
            .master(master_url)
            .config("spark.kubernetes.executor.container.image", EXECUTOR_IMAGE)
            .config("spark.driver.host", driver_host)
            .config("spark.kubernetes.namespace", driver_namespace)
            .config("spark.executor.instances", self.executor_instances)
            .config("spark.executor.memory", self.executor_memory)
            .config("spark.executor.cores", self.executor_cores)
            .config(
                "spark.kubernetes.executor.podTemplateFile",
                self._create_executor_pod_template_file(
                    driver_namespace, in_cluster=True
                ),
            )
            .config(
                "spark.kubernetes.container.image.pullSecrets", "docker-registry-creds"
            )  # this should point to the same value as what's configured in the respective namespace
        )

        if self.spark_jars_packages:
            builder = builder.config("spark.jars.packages", self.spark_jars_packages)

        if self.additional_spark_conf:
            for key, value in self.additional_spark_conf.items():
                builder = builder.config(key, value)

        self.spark = builder.getOrCreate()

        logger.info("Successfully created spark session")

        return self.spark

    def __exit__(self, exc_type, exc_value, traceback):
        if self.spark:
            logger.info("Stopping spark session")
            self.spark.stop()
            self.spark = None

    def _create_executor_pod_template_file(
        self,
        driver_namespace,
        in_cluster=True,
    ):
        pod_spec_dict = build_executor_pod_spec(
            application_name=self.app_name,
            in_cluster=in_cluster,
            namespace=driver_namespace,
            secrets=(
                os.environ.get("ORCH_SPARK_K8S_ENVS_LIST").split(",")
                if os.environ.get("ORCH_SPARK_K8S_ENVS_LIST")
                else None
            ),
        )

        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".yaml", mode="w"
        ) as tmpfile:
            yaml.dump(pod_spec_dict, tmpfile, default_flow_style=False)
            temp_file_path = tmpfile.name

        logger.info("Executor pod spec written to temp file %s", temp_file_path)
        return temp_file_path
