# orchestera-lib

## Releasing

Bump `pyproject.toml` and `uv.lock` in a PR, review and merge it. From a clean,
up-to-date local `main` with `uv` and an authenticated `gh` CLI, run:

```bash
make release
```

This checks the lockfile, runs the unit tests, builds the distributions, and
creates `v<project-version>` from the current `main` commit. Publishing that
GitHub Release triggers `.github/workflows/publish.yml` to upload to PyPI once;
merging the version-bump PR alone does **not** publish. Confirm that workflow
succeeds and the wheel appears on PyPI before updating the Docker image pin.
The target refuses a dirty/out-of-date checkout or an existing remote tag.
For prerelease versions it marks the GitHub Release as a prerelease.

## Spark runtime configuration

`OrchesteraSparkSession` creates a client-mode Spark session from inside an
Orchestera notebook pod. It never selects an image, registry secret, service
account, or node placement on its own:

- `ORCH_SPARK_K8S_CONTAINER_IMAGE` is required and must be the digest-pinned
  driver/executor image supplied by the notebook provisioner.
- `ORCH_SPARK_K8S_NAMESPACE` is required; `ORCH_SPARK_K8S_SERVICE_ACCOUNT`
  defaults to `workload` and can be overridden for another tenant contract.
- Executor pods default to the tenant NodePool named by `ORCH_SPARK_K8S_NAMESPACE`,
  with its matching `orchestera.com/namespace` NoSchedule toleration. This keeps
  executors in the same workspace as the notebook without hardcoding a namespace.
  `ORCH_SPARK_K8S_NODE_SELECTOR` and `ORCH_SPARK_K8S_TOLERATIONS` are optional
  JSON overrides for clusters with a different placement contract; explicit
  session arguments override those environment values.
- The executor CPU limit defaults to `executor_cores` to satisfy tenant
  ResourceQuota; `additional_spark_conf` can override
  `spark.kubernetes.executor.limit.cores` if needed.
- `ORCH_SPARK_EVENT_LOG_DIR` is optional. Event logging is disabled unless a
  directory is explicitly supplied.

The image must already contain matching Spark/PySpark and S3A/JDBC
requirements. The library does not install packages at runtime or inject image
pull credentials.

## External connections

The control plane mounts the workspace's S3 and database connections into the
pod as a read-only `connections.json`, pointed at by `ORCH_CONNECTIONS_FILE`
(default `/var/run/orchestera/connections/connections.json`). The file holds
only the connections of the workspace its namespace is bound to.

`OrchesteraSparkSession` reads it when the session is created and applies each
S3 connection's per-bucket `spark.hadoop.fs.s3a.bucket.<bucket>.*` config, so
`s3a://` paths work with no setup in the notebook:

```python
with OrchesteraSparkSession(app_name="etl", executor_instances=2,
                            executor_cores=1, executor_memory="2g") as spark:
    df = spark.read.parquet("s3a://analytics/events/")
```

Those keys are per bucket, so they take precedence for their own bucket while
leaving the global Pod Identity credentials provider in place for every other
bucket. An explicit `additional_spark_conf` entry still overrides both. Because
the file is read at session creation, a connection added after the notebook
started is picked up by the next session, once the kubelet has refreshed the
mounted Secret (about a minute).

Database connections are not applied automatically -- read one by name:

```python
from orchestera.connections import get_connection

conn = get_connection("warehouse")
df = spark.read.format("jdbc").options(**conn.jdbc).option("dbtable", "public.orders").load()
```

A missing file means the workspace has no connections and is not an error. A
file that exists but cannot be parsed raises `ConnectionsError` rather than
starting a session that would fail later with a confusing permission error.

## Minimal TUI

Run from a local checkout:

```bash
uv run python -m orchestera.tui.cli
```

Run after install (console script):

```bash
pip install orchestera-lib
orchestera-tui
```

Run with `uvx`:

```bash
uvx --from orchestera-lib orchestera-tui
```

Run with `uvx` from the current local repo:

```bash
uvx --from . orchestera-tui
```

Inside the TUI, run:

```text
/start-proxy
```

Stop it with:

```text
/stop-proxy
```

Update kube context with:

```text
/update-cluster-context <cluster-name>
```

Upload IAM policy JSON to Parameter Store with:

```text
/upload-iam-policy <cluster-name> <namespace> <alias>
```

Upload secrets JSON to Parameter Store with:

```text
/upload-secrets <cluster-name> <namespace> <alias>
```

Exit the TUI with:

```text
/exit
```

Commands support fuzzy matching, so close variants like `/strt-proxy` also work.

`/update-cluster-context <cluster-name>` runs:

```bash
aws eks update-kubeconfig --name <cluster-name> --region us-east-1 --profile orchestera-dev
```

`/upload-iam-policy <cluster-name> <namespace> <alias>` writes:

```text
/orchestera/sparklith/<cluster-name>/<namespace>/iams/<alias>
```

`/upload-secrets <cluster-name> <namespace> <alias>` writes:

```text
/orchestera/sparklith/<cluster-name>/<namespace>/secrets/<alias>
```

For both upload commands:

- TUI opens a multiline JSON editor.
- `Ctrl+S` validates and uploads.
- `Esc` cancels.
- Uses AWS profile `orchestera-useradmin`, region `us-east-1`, and overwrites existing values.

This starts:

```bash
kubectl port-forward svc/traefik -n traefik 8080:80 3080:3080 9001:8080
```

Forwarded local ports:

- `8080`
- `3080`
- `9001`
