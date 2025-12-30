import logging
import uuid
from functools import cached_property

import click
from kubernetes import config
from kubernetes.client import CoreV1Api, V1Pod

from orchestera.kubernetes.pod_manager import DefaultPodManager
from orchestera.kubernetes.pod_spec_builder import build_driver_pod_spec

logger = logging.getLogger(__name__)


class SparklithKubernetesClientConfiguration:

    def __init__(self, context=None):
        # Load from default kubeconfig (~/.kube/config)
        config.load_kube_config(context=context)


class SparklithKubernetesClient:
    def __init__(self, configuration) -> None:
        self.configuration = configuration

    @cached_property
    def pod_manager(self):
        return DefaultPodManager(self.k8s_client)

    @cached_property
    def k8s_client(self):
        return CoreV1Api()

    def build_pod_request_obj(self, entrypoint, context) -> V1Pod:

        user_labels = context.get("labels", {})
        namespace = context.get("namespace")
        secret_names = context.get("secrets", [])

        pod = build_driver_pod_spec(
            application_name=context["application_name"],
            image=context["image"],
            memory_request=context["memory_request"],
            memory_limit=context["memory_limit"],
            cpu_request=context["cpu_request"],
            cpu_limit=context["cpu_limit"],
            in_cluster=True,
            namespace=namespace,
            secrets=secret_names,
        )

        pod.spec.containers[0].command = entrypoint

        # Ensure pod.metadata.labels is initialized
        if pod.metadata.labels is None:
            pod.metadata.labels = {}

        pod.metadata.labels.update(
            {
                **user_labels,
            }
        )

        final_lables = pod.metadata.labels

        sorted_labels = sorted(final_lables.items())
        labels_str = "".join(f"{key}-{value}" for key, value in sorted_labels)

        pod.metadata.labels["deterministic_pod_id"] = str(
            uuid.uuid5(uuid.NAMESPACE_DNS, labels_str)
        )[:8]

        return pod

    def find_pod(self, pod_request_obj, context):
        namespace = pod_request_obj.metadata.namespace
        labels = pod_request_obj.metadata.labels
        label_strings = [
            f"{label_id}={label}" for label_id, label in sorted(labels.items())
        ]
        labels_value = ",".join(label_strings)

        pod_list = self.k8s_client.list_namespaced_pod(
            namespace=namespace,
            label_selector=labels_value,
        ).items

        if len(pod_list) == 1:
            return pod_list[0]
        elif len(pod_list) > 1:
            raise Exception(
                f"Found multiple pods with labels {labels_value} in namespace {namespace}"
            )
        else:
            return None

    def get_or_create_pod(self, pod_request_obj, context):
        pod = self.find_pod(pod_request_obj, context=context)
        if not pod:
            pod = self.pod_manager.create_pod(pod_request_obj, context=context)

        return pod

    def await_pod_start(self, pod):
        try:
            self.pod_manager.await_pod_start(
                pod, startup_timeout=300, startup_check_interval=2
            )
        except Exception as e:
            logger.error(f"Error waiting for pod to start: {e}")
            raise

    def await_pod_completion(self, pod):
        return self.pod_manager.await_pod_completion(pod)

    def _raise_on_nonzero_exit(self, remote_pod: V1Pod):
        """Inspect container termination statuses and raise on non-zero exit.

        - If any container terminated with exit_code != 0, raise RuntimeError
        - If pod phase is Failed, also raise with details
        """
        phase = getattr(remote_pod.status, "phase", None)
        container_statuses = (
            getattr(remote_pod.status, "container_statuses", None) or []
        )

        non_zero_details = []
        for status in container_statuses:
            state = getattr(status, "state", None)
            terminated = getattr(state, "terminated", None) if state else None
            if terminated is not None:
                code = getattr(terminated, "exit_code", None)
                if code is not None and int(code) != 0:
                    name = getattr(status, "name", "<unknown>")
                    reason = getattr(terminated, "reason", None)
                    message = getattr(terminated, "message", None)
                    non_zero_details.append(
                        f"{name}=exit_code:{code} reason:{reason or ''} message:{(message or '').strip()}"
                    )

        if phase == "Failed" or non_zero_details:
            details = "; ".join(non_zero_details) if non_zero_details else ""
            pod_name = getattr(remote_pod.metadata, "name", "<unknown>")
            raise RuntimeError(
                f"Pod {pod_name} completed unsuccessfully (phase={phase}). {details}".strip()
            )

    def cleanup(self, remote_pod):
        if not remote_pod:
            return

        self.pod_manager.delete_pod(remote_pod)

    def execute(self, entrypoint, context):
        pod_request_obj = self.build_pod_request_obj(entrypoint, context)

        pod = self.get_or_create_pod(pod_request_obj, context)

        self.await_pod_start(pod)

        remote_pod = None
        try:
            remote_pod = self.await_pod_completion(pod)
            # Evaluate container exit codes and pod phase; raise if any non-zero
            self._raise_on_nonzero_exit(remote_pod)
        finally:
            # Always attempt cleanup
            self.cleanup(remote_pod or pod)


@click.group()
def cli():
    """Orchestera sync client CLI."""
    pass


@cli.command()
@click.option(
    "--kube-context",
    default=None,
    help="Kubernetes context to use from kubeconfig",
)
@click.option(
    "--pull-secrets-from",
    default=None,
    help="Comma-separated list of secrets to pull from",
)
@click.option(
    "--application-name",
    default="spark-app",
    help="Name of the Spark application",
)
@click.option(
    "--image",
    required=True,
    help="Docker image to use for the Spark application",
)
@click.option(
    "--memory-request",
    default="1G",
    help="Memory request for the Spark application",
)
@click.option(
    "--memory-limit",
    default="2G",
    help="Memory limit for the Spark application",
)
@click.option(
    "--cpu-request",
    default="1",
    help="CPU request for the Spark application",
)
@click.option(
    "--cpu-limit",
    default="2",
    help="CPU limit for the Spark application",
)
@click.option(
    "--namespace",
    required=True,
    help="Kubernetes namespace to deploy to",
)
@click.option(
    "--classpath",
    required=True,
    help="Python classpath for the Spark application (e.g., example.spark.application.SparkK8sHelloWorld)",
)
def run(
    kube_context,
    pull_secrets_from,
    application_name,
    image,
    memory_request,
    memory_limit,
    cpu_request,
    cpu_limit,
    namespace,
    classpath,
):
    """Run a Spark application on Kubernetes using kubeconfig."""
    click.echo("Running...")

    secrets = pull_secrets_from.split(",") if pull_secrets_from else None

    k8s_config = SparklithKubernetesClientConfiguration(context=kube_context)

    sparklith_client = SparklithKubernetesClient(k8s_config)

    context = {
        "application_name": application_name,
        "image": image,
        "memory_request": memory_request,
        "memory_limit": memory_limit,
        "cpu_request": cpu_request,
        "cpu_limit": cpu_limit,
        "namespace": namespace,
        "secrets": secrets,
    }

    sparklith_client.execute(
        entrypoint=[
            "python3",
            "-m",
            "orchestera.entrypoints.run",
            classpath,
            "--application-name",
            application_name,
        ],
        context=context,
    )


if __name__ == "__main__":
    cli()
