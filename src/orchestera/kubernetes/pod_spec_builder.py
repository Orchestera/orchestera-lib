"""
Utility functions to manage Kubernetes pod specifications for Spark applications.
"""

from typing import List, Optional
from kubernetes.client import (
    V1Container,
    V1LocalObjectReference,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
    V1Toleration,
    V1EnvVar,
    V1EnvFromSource,
    V1SecretEnvSource,
)


def build_driver_pod_spec(
    *,
    application_name,
    image,
    memory_request,
    memory_limit,
    cpu_request,
    cpu_limit,
    in_cluster,
    namespace,
    secrets: Optional[List[str]] = None,
) -> dict:
    pod_spec = V1PodSpec(
        # TODO: Not sure how this will work in context of AWS docker so fix it
        image_pull_secrets=[V1LocalObjectReference(name="docker-registry-creds")],
        service_account_name="spark",
        containers=[
            V1Container(
                name="spark-driver",
                image=image,
                image_pull_policy="Always",
                # TODO: This path needs to be fixed
                command=["python3", "app/src/sparkeum/spark/application.py"],
                resources=V1ResourceRequirements(
                    requests={"memory": memory_request, "cpu": cpu_request},
                    limits={"memory": memory_limit, "cpu": cpu_limit},
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

    if secrets:
        env_from_sources = [
            V1EnvFromSource(secret_ref=V1SecretEnvSource(name=secret))
            for secret in secrets
            if isinstance(secret, str)
        ]
        if env_from_sources:
            existing_env_from = getattr(pod_spec.containers[0], "env_from", None) or []
            pod_spec.containers[0].env_from = list(existing_env_from) + env_from_sources

    if in_cluster:
        pod_spec.node_selector = {"dedicated": "spark"}
        pod_spec.tolerations = [
            V1Toleration(
                key="dedicated",
                operator="Equal",
                value="spark",
                effect="NoSchedule",
            )
        ]

    pod = V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=V1ObjectMeta(
            name=application_name,
            namespace=namespace,
        ),
        spec=pod_spec,
    )

    return pod


def build_executor_pod_spec(
    *,
    application_name: str,
    in_cluster: bool,
    secrets: Optional[List[str]] = None,
) -> dict:
    """
    Generate a Kubernetes pod spec for a Spark executor as a dictionary.
    """
    executor_pod_spec = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "labels": {
                "application_name": application_name,
            },
        },
        "spec": {
            "containers": [
                {
                    "name": "spark-executor",
                    "env": [
                        {"name": "HOME", "value": "/tmp"},
                        {"name": "PYSPARK_PYTHON", "value": "python3"},
                    ],
                    # Add any additional container specs if needed
                }
            ],
            "nodeSelector": {"dedicated": "spark"},
            "tolerations": [
                {
                    "key": "dedicated",
                    "operator": "Equal",
                    "value": "spark",
                    "effect": "NoSchedule",
                }
            ],
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

    if not in_cluster:
        executor_pod_spec["spec"].pop("nodeSelector")
        executor_pod_spec["spec"].pop("tolerations")

    return executor_pod_spec
