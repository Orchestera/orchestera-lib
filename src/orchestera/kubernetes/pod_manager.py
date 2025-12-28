import logging
import time
from urllib.error import HTTPError

import tenacity
from kubernetes import client
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class PodPhase:
    """
    Possible pod phases.

    See https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase.
    """

    PENDING = "Pending"
    RUNNING = "Running"
    FAILED = "Failed"
    SUCCEEDED = "Succeeded"

    terminal_states = {FAILED, SUCCEEDED}


class DefaultPodManager:

    def __init__(self, kube_client):
        self._client = kube_client

    def await_pod_start(self, pod, startup_timeout=180, startup_check_interval=2):
        curr_time = time.time()
        while True:
            remote_pod = self.read_pod(pod)

            # Early detection for fatal Waiting reasons on (init_)containers
            fatal_reason = self._detect_fatal_waiting_reason(remote_pod)
            if fatal_reason is not None:
                raise RuntimeError(
                    f"Pod {pod.metadata.name} cannot start due to container Waiting reason: {fatal_reason}"
                )

            if remote_pod.status.phase != PodPhase.PENDING:
                break
            logger.warning(
                f"Pod {pod.metadata.name} is still in {PodPhase.PENDING} phase. Waiting..."
            )
            if time.time() - curr_time > startup_timeout:
                raise TimeoutError(
                    f"Pod {pod.metadata.name} did not start within {startup_timeout} seconds"
                )
            time.sleep(startup_check_interval)

    def await_pod_completion(self, pod):
        while True:
            remote_pod = self.read_pod(pod)
            if remote_pod.status.phase in PodPhase.terminal_states:
                break
            logger.info(
                f"Pod {pod.metadata.name} has phase {remote_pod.status.phase}. Waiting..."
            )
            time.sleep(2)
        return remote_pod

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True,
    )
    def read_pod(self, pod):
        """Read POD information."""
        try:
            return self._client.read_namespaced_pod(
                pod.metadata.name, pod.metadata.namespace
            )
        except HTTPError as e:
            raise Exception(f"There was an error reading the kubernetes API: {e}")

    def create_pod(self, pod_request_obj, context):
        namespace = pod_request_obj.metadata.namespace

        try:
            pod = self._client.create_namespaced_pod(
                namespace=namespace, body=pod_request_obj
            )
            logger.info(f"Pod {pod_request_obj.metadata.name} created successfully.")
        except client.rest.ApiException as e:
            logger.error(f"Error creating pod: {e}")
            raise

        return pod

    def delete_pod(self, pod):
        try:
            self._client.delete_namespaced_pod(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                body=client.V1DeleteOptions(),
            )
        except ApiException as e:
            # If the pod is already deleted
            if str(e.status) != "404":
                raise

    def _detect_fatal_waiting_reason(self, remote_pod):
        """
        Inspect pod container and initContainer statuses for fatal Waiting reasons
        that indicate the container will never start without human action.

        Returns a string with details if found, otherwise None.
        """
        fatal_waiting_reasons = {
            "CreateContainerConfigError",
            "CreateContainerError",
            "ImagePullBackOff",
            "ErrImagePull",
            "InvalidImageName",
            "CrashLoopBackOff",
        }

        def collect(statuses):
            details = []
            for status in statuses or []:
                state = getattr(status, "state", None)
                waiting = getattr(state, "waiting", None) if state else None
                if waiting is not None:
                    reason = getattr(waiting, "reason", None)
                    if reason in fatal_waiting_reasons:
                        name = getattr(status, "name", "<unknown>")
                        message = getattr(waiting, "message", "") or ""
                        details.append(
                            f"{name} reason={reason} message={(message.strip())}"
                        )
            return details

        container_details = collect(
            getattr(remote_pod.status, "container_statuses", None)
        )
        init_container_details = collect(
            getattr(remote_pod.status, "init_container_statuses", None)
        )

        all_details = container_details + init_container_details
        if all_details:
            return "; ".join(all_details)
        return None
