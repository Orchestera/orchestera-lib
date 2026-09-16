"""
Utility functions to manage Kubernetes pod specifications for Spark applications.
"""

# The package dependencies are installed in the project uv environment. Some
# editor hosts do not expose that environment to Pyright.
# pyright: reportMissingImports=false

from typing import Any, Mapping, Optional, Sequence

from kubernetes.client import (
    V1Container,
    V1EnvFromSource,
    V1EnvVar,
    V1LocalObjectReference,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
    V1SecretEnvSource,
    V1Toleration,
)


def build_driver_pod_spec(
    *,
    application_name,
    image,
    memory_request,
    cpu_request,
    in_cluster,
    namespace,
    secrets: Optional[Sequence[str]] = None,
    service_account_name: Optional[str] = None,
    image_pull_secrets: Optional[Sequence[str]] = None,
    node_selector: Optional[Mapping[str, str]] = None,
    tolerations: Optional[Sequence[Mapping[str, Any]]] = None,
) -> V1Pod:
    pod_spec = V1PodSpec(
        image_pull_secrets=(
            [V1LocalObjectReference(name=name) for name in image_pull_secrets]
            if image_pull_secrets
            else None
        ),
        service_account_name=service_account_name,
        containers=[
            V1Container(
                name="spark-driver",
                image=image,
                image_pull_policy="Always",
                # TODO: This path needs to be fixed
                command=["python3", "app/src/sparkeum/spark/application.py"],
                resources=V1ResourceRequirements(
                    requests={"memory": memory_request, "cpu": cpu_request},
                ),
                env=[
                    V1EnvVar(name="ORCH_SPARK_K8S_NAMESPACE", value=namespace),
                ]
                + (
                    [V1EnvVar(name="ORCH_SPARK_K8S_ENVS_LIST", value=",".join(secrets))]
                    if secrets
                    else []
                ),
            )
        ],
        restart_policy="Never",
    )

    if secrets and pod_spec.containers:
        env_from_sources = [
            V1EnvFromSource(secret_ref=V1SecretEnvSource(name=secret))
            for secret in secrets
            if isinstance(secret, str)
        ]
        if env_from_sources:
            container = pod_spec.containers[0]
            existing_env_from = container.env_from or []
            container.env_from = list(existing_env_from) + env_from_sources

    if node_selector:
        pod_spec.node_selector = dict(node_selector)
    if tolerations:
        pod_spec.tolerations = [
            V1Toleration(**dict(toleration)) for toleration in tolerations
        ]

    pod = V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=V1ObjectMeta(
            name=application_name,
            namespace=namespace,
            labels={
                "application_name": application_name,
                "namespace": namespace,
                "spark-role": "driver",
            },
        ),
        spec=pod_spec,
    )

    return pod


def build_executor_pod_spec(
    *,
    application_name: str,
    in_cluster: bool,
    namespace: str,
    secrets: Optional[Sequence[str]] = None,
    service_account_name: Optional[str] = None,
    node_selector: Optional[Mapping[str, str]] = None,
    tolerations: Optional[Sequence[Mapping[str, Any]]] = None,
    python_executable: str = "python3",
) -> dict[str, Any]:
    """Generate a tenant-configured Kubernetes pod template for Spark executors."""
    executor_pod_spec: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "labels": {
                "application_name": application_name,
                "namespace": namespace,
                "spark-role": "executor",
            },
        },
        "spec": {
            "containers": [
                {
                    "name": "spark-executor",
                    "env": [
                        {"name": "HOME", "value": "/tmp"},
                        {"name": "PYSPARK_PYTHON", "value": python_executable},
                    ],
                    # Add any additional container specs if needed
                }
            ],
            **(
                {"serviceAccountName": service_account_name}
                if service_account_name
                else {}
            ),
            **({"nodeSelector": dict(node_selector)} if node_selector else {}),
            **(
                {"tolerations": [dict(toleration) for toleration in tolerations]}
                if tolerations
                else {}
            ),
        },
    }

    # Add secrets as envFrom
    if secrets:
        env_from_list = [
            {"secretRef": {"name": secret}}
            for secret in secrets
            if isinstance(secret, str)
        ]
        if env_from_list:
            executor_container = executor_pod_spec["spec"]["containers"][0]
            if "envFrom" in executor_container and isinstance(
                executor_container["envFrom"], list
            ):
                executor_container["envFrom"].extend(env_from_list)
            else:
                executor_container["envFrom"] = env_from_list

    return executor_pod_spec
