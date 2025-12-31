import logging
import os
import socket
import tempfile

import yaml
from kubernetes import config as kubernetes_config
from pyspark.sql import SparkSession

from orchestera.kubernetes.pod_spec_builder import build_executor_pod_spec

logger = logging.getLogger(__name__)

EXECUTOR_IMAGE = "ghcr.io/orchestera/docker-images/spark:3.5.6"


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
        self.additional_spark_conf = additional_spark_conf or {}

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
        )

        default_spark_conf = self._default_spark_confs()
        default_spark_conf.update(self.additional_spark_conf)

        for key, value in default_spark_conf.items():
            builder = builder.config(key, value)

        if self.spark_jars_packages:
            builder = builder.config("spark.jars.packages", self.spark_jars_packages)

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

    def _default_spark_confs(self):
        return {
            "spark.default.parallelism": 4,
            # Explicitly add the JARs to executor classpath
            "spark.executor.extraClassPath": "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.746.jar",
            # Allow EKS Pod Identity agent FULL_URI host for AWS SDK v1
            "spark.driver.extraJavaOptions": "-Dcom.amazonaws.sdk.ecsFullUriAllowedHosts=169.254.170.23,localhost,127.0.0.1",
            "spark.executor.extraJavaOptions": "-Dcom.amazonaws.sdk.ecsFullUriAllowedHosts=169.254.170.23,localhost,127.0.0.1",
            # Service account for pod identity
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.kubernetes.authenticate.executor.serviceAccountName": "spark",
            # Hadoop AWS filesystem
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            # Use EKS Pod Identity container credentials
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper"
            ),
            # Prevent IMDS from being used as a fallback so node instance profile doesn't override Pod Identity
            "spark.executorEnv.AWS_EC2_METADATA_DISABLED": "true",
            "spark.kubernetes.driverEnv.AWS_EC2_METADATA_DISABLED": "true",
            # Optional: faster committers for Spark
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
            "spark.hadoop.fs.s3a.committer.name": "directory",
            "spark.hadoop.fs.s3a.committer.magic.enabled": "false",
            # Set HOME to writable directory for executors
            "spark.executorEnv.HOME": "/tmp",
            "spark.executorEnv.PYSPARK_PYTHON": "python3",
        }
