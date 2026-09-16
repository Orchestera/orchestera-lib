import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from orchestera.kubernetes.pod_spec_builder import (
    build_driver_pod_spec,
    build_executor_pod_spec,
)
from orchestera.spark.session import OrchesteraSparkSession, get_kubernetes_host_addr


class RecordingBuilder:
    def __init__(self):
        self.configurations = {}
        self.spark = SimpleNamespace(stop=lambda: None)

    def appName(self, value):
        self.configurations["appName"] = value
        return self

    def master(self, value):
        self.configurations["master"] = value
        return self

    def config(self, key, value):
        self.configurations[key] = value
        return self

    def getOrCreate(self):
        return self.spark


class PodSpecBuilderTests(unittest.TestCase):
    def test_driver_defaults_do_not_assume_registry_account_or_placement(self):
        pod = build_driver_pod_spec(
            application_name="job",
            image="ghcr.io/orchestera/docker-images/spark@sha256:" + "a" * 64,
            memory_request="1Gi",
            cpu_request="1",
            in_cluster=True,
            namespace="tenant-a",
        )

        spec = pod.spec
        assert spec is not None
        self.assertIsNone(spec.service_account_name)
        self.assertIsNone(spec.image_pull_secrets)
        self.assertIsNone(spec.node_selector)
        self.assertIsNone(spec.tolerations)

    def test_executor_uses_explicit_tenant_placement(self):
        pod = build_executor_pod_spec(
            application_name="job",
            in_cluster=True,
            namespace="tenant-a",
            service_account_name="workload",
            node_selector={"karpenter.sh/nodepool": "tenant-a"},
            tolerations=[
                {
                    "key": "orchestera.com/namespace",
                    "operator": "Equal",
                    "value": "tenant-a",
                    "effect": "NoSchedule",
                }
            ],
            python_executable="/opt/venv/bin/python",
        )

        spec = pod["spec"]
        self.assertEqual(spec["serviceAccountName"], "workload")
        self.assertEqual(spec["nodeSelector"], {"karpenter.sh/nodepool": "tenant-a"})
        self.assertEqual(spec["tolerations"][0]["value"], "tenant-a")
        self.assertEqual(
            spec["containers"][0]["env"][1],
            {"name": "PYSPARK_PYTHON", "value": "/opt/venv/bin/python"},
        )


class SparkSessionTests(unittest.TestCase):
    def test_missing_cluster_endpoint_fails_clearly(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "KUBERNETES_SERVICE_HOST"):
                get_kubernetes_host_addr()

    def test_missing_executor_image_fails_before_creating_spark(self):
        with patch.dict(
            os.environ,
            {
                "KUBERNETES_SERVICE_HOST": "kubernetes.default.svc",
                "KUBERNETES_SERVICE_PORT": "443",
                "ORCH_SPARK_K8S_NAMESPACE": "tenant-a",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "ORCH_SPARK_K8S_CONTAINER_IMAGE"):
                OrchesteraSparkSession(
                    app_name="smoke",
                    executor_instances=1,
                    executor_cores=1,
                    executor_memory="512m",
                ).__enter__()

    def test_session_uses_notebook_image_digest_and_tenant_workload_settings(self):
        builder = RecordingBuilder()
        environment = {
            "KUBERNETES_SERVICE_HOST": "kubernetes.default.svc",
            "KUBERNETES_SERVICE_PORT": "443",
            "ORCH_SPARK_K8S_NAMESPACE": "tenant-a",
            "ORCH_SPARK_K8S_CONTAINER_IMAGE": "ghcr.io/orchestera/docker-images/spark@sha256:"
            + "a" * 64,
            "ORCH_SPARK_K8S_SERVICE_ACCOUNT": "workload",
            "ORCH_SPARK_K8S_NODE_SELECTOR": '{"karpenter.sh/nodepool":"tenant-a"}',
            "ORCH_SPARK_K8S_TOLERATIONS": '[{"key":"orchestera.com/namespace","operator":"Equal","value":"tenant-a","effect":"NoSchedule"}]',
            "SPARK_DRIVER_BIND_ADDRESS": "10.0.0.4",
            "PYSPARK_PYTHON": "/opt/venv/bin/python",
        }
        fake_spark_session = SimpleNamespace(builder=builder)

        with (
            patch.dict(os.environ, environment, clear=True),
            patch("orchestera.spark.session.SparkSession", fake_spark_session),
        ):
            session = OrchesteraSparkSession(
                app_name="smoke",
                executor_instances=1,
                executor_cores=1,
                executor_memory="512m",
            )
            spark = session.__enter__()
            template_path = builder.configurations[
                "spark.kubernetes.executor.podTemplateFile"
            ]
            with open(template_path) as template:
                pod_template = yaml.safe_load(template)
            assert isinstance(pod_template, dict)
            pod_spec = pod_template.get("spec")
            assert isinstance(pod_spec, dict)
            session.__exit__(None, None, None)

        self.assertIs(spark, builder.spark)
        self.assertEqual(builder.configurations["appName"], "smoke")
        self.assertEqual(builder.configurations["spark.submit.deployMode"], "client")
        self.assertEqual(
            builder.configurations["spark.kubernetes.container.image"],
            environment["ORCH_SPARK_K8S_CONTAINER_IMAGE"],
        )
        self.assertEqual(
            builder.configurations[
                "spark.kubernetes.authenticate.driver.serviceAccountName"
            ],
            "workload",
        )
        self.assertEqual(builder.configurations["spark.driver.host"], "10.0.0.4")
        self.assertNotIn("spark.eventLog.enabled", builder.configurations)
        self.assertEqual(pod_spec["serviceAccountName"], "workload")
        self.assertEqual(
            pod_spec["nodeSelector"], {"karpenter.sh/nodepool": "tenant-a"}
        )
        self.assertFalse(os.path.exists(template_path))

    def test_event_log_is_opt_in(self):
        session = OrchesteraSparkSession(
            app_name="smoke",
            executor_instances=1,
            executor_cores=1,
            executor_memory="512m",
            event_log_dir="s3a://logs/spark",
        )
        self.assertEqual(session.event_log_dir, "s3a://logs/spark")
        self.assertNotIn("spark.eventLog.enabled", session._default_spark_confs())


if __name__ == "__main__":
    unittest.main()
