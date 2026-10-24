"""
Metrics for one notebook iteration, read from the live driver's REST API.

The notebook is the Spark driver (client mode), so its own UI server at
`spark.sparkContext.uiWebUrl` serves the same `/api/v1` the History Server
does, for the running application and with no delay. The History Server
can't stand in for it: event logs go to S3 through s3a, which has no
hflush, so a session's `.inprogress` log only lands in the bucket when the
session stops -- and a notebook session rarely stops between iterations.

An iteration is every job that finished after the last analysed one. Only
the summary below goes to the model; it reads task-level detail through the
tools in `job_analysis` when it needs it.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

# Stages that get a task distribution (the skew signal), by task time.
TOP_STAGES = 8
EXECUTOR_LIMIT = 20
SQL_PLAN_CHARS = 4000
FAILURE_CHARS = 1500
TASK_QUANTILES = "0.0,0.25,0.5,0.75,1.0"

# Conf the model may see. Everything else (credentials, hadoop and s3a
# settings, paths) stays out even though the driver already redacts secrets.
CONF_PREFIXES = (
    "spark.sql.adaptive.",
    "spark.sql.shuffle.partitions",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.files.maxPartitionBytes",
    "spark.executor.instances",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.driver.memory",
    "spark.driver.cores",
    "spark.default.parallelism",
    "spark.dynamicAllocation.",
    "spark.memory.",
    "spark.shuffle.",
    "spark.serializer",
    "spark.speculation",
)
REDACTED_MARKER = "(redacted)"

FINISHED_JOB_STATES = {"SUCCEEDED", "FAILED"}


class SparkRestError(RuntimeError):
    pass


class SparkRestClient:
    """`GET /api/v1/applications/<app>/...` on the driver's own UI server."""

    def __init__(self, ui_url: str, app_id: str, timeout: float = 10.0) -> None:
        self.base = f"{ui_url.rstrip('/')}/api/v1/applications/{app_id}"
        self.app_id = app_id
        self.timeout = timeout

    @classmethod
    def from_spark(cls, spark: Any) -> SparkRestClient:
        sc = spark.sparkContext
        if not sc.uiWebUrl:
            raise SparkRestError(
                "The Spark UI is disabled (spark.ui.enabled=false); job analysis "
                "reads the running session's metrics from it."
            )
        # uiWebUrl names the pod IP, and whether the namespace's default-deny
        # NetworkPolicy lets a pod reach its own IP is up to the CNI. The UI
        # binds 0.0.0.0 unless SPARK_LOCAL_IP pins it, so loopback comes first.
        port = urllib.parse.urlsplit(sc.uiWebUrl).port
        for ui_url in (f"http://127.0.0.1:{port}", sc.uiWebUrl):
            client = cls(ui_url, sc.applicationId)
            try:
                client.get("/environment")
            except SparkRestError:
                continue
            return client
        raise SparkRestError(f"The Spark UI at {sc.uiWebUrl} does not answer.")

    def get(self, path: str, **params: Any) -> Any:
        url = self.base + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as response:
                return json.load(response)
        except OSError as exc:
            raise SparkRestError(f"Spark UI request {path} failed: {exc}") from exc


@dataclass
class IterationSnapshot:
    app_id: str
    app_name: str
    job_ids: list[int]
    jobs: list[dict]
    stages: list[dict]
    sql: list[dict]
    executors: list[dict]
    conf: dict[str, str]
    totals: dict[str, int]
    # Jobs still running when the snapshot was taken; left for the next one.
    running_job_ids: list[int] = field(default_factory=list)

    def for_prompt(self) -> dict:
        return {
            "app_id": self.app_id,
            "app_name": self.app_name,
            "job_ids": self.job_ids,
            "totals": self.totals,
            "jobs": self.jobs,
            "stages": self.stages,
            "sql_executions": self.sql,
            "executors": self.executors,
            "spark_conf": self.conf,
            "still_running_job_ids": self.running_job_ids,
        }


def parse_time(value: str | None) -> datetime | None:
    # Spark's REST dates: 2026-10-24T03:31:49.787GMT
    if not value:
        return None
    return datetime.strptime(value.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z")


def _ms_between(start: str | None, end: str | None) -> int | None:
    started, ended = parse_time(start), parse_time(end)
    if not started or not ended:
        return None
    return int((ended - started).total_seconds() * 1000)


def _truncate(text: str | None, limit: int) -> str | None:
    if text is None or len(text) <= limit:
        return text
    return text[:limit] + f"\n... [{len(text) - limit} more characters]"


def _skew(values: list[float]) -> float | None:
    # max / median of a quantile list ordered as TASK_QUANTILES.
    if len(values) < 5 or not values[2]:
        return None
    return round(values[4] / values[2], 1)


def _task_distribution(summary: dict) -> dict:
    shuffle_read = summary.get("shuffleReadMetrics", {}).get("readBytes", [])
    return {
        "quantiles": summary.get("quantiles"),
        "duration_ms": summary.get("duration"),
        "run_time_ms": summary.get("executorRunTime"),
        "gc_ms": summary.get("jvmGcTime"),
        "shuffle_read_bytes": shuffle_read,
        "spill_disk_bytes": summary.get("diskBytesSpilled"),
        "run_time_skew_max_over_median": _skew(summary.get("executorRunTime", [])),
        "shuffle_read_skew_max_over_median": _skew(shuffle_read),
    }


def _compact_stage(stage: dict) -> dict:
    return {
        "stage_id": stage["stageId"],
        "attempt_id": stage["attemptId"],
        "name": stage.get("name"),
        "status": stage.get("status"),
        "num_tasks": stage.get("numTasks"),
        "failed_tasks": stage.get("numFailedTasks"),
        "killed_tasks": stage.get("numKilledTasks"),
        "duration_ms": _ms_between(
            stage.get("submissionTime"), stage.get("completionTime")
        ),
        "task_time_ms": stage.get("executorRunTime"),
        "cpu_time_ms": int(stage.get("executorCpuTime", 0) / 1_000_000),
        "gc_ms": stage.get("jvmGcTime"),
        "input_bytes": stage.get("inputBytes"),
        "output_bytes": stage.get("outputBytes"),
        "shuffle_read_bytes": stage.get("shuffleReadBytes"),
        "shuffle_fetch_wait_ms": stage.get("shuffleFetchWaitTime"),
        "shuffle_write_bytes": stage.get("shuffleWriteBytes"),
        "memory_spill_bytes": stage.get("memoryBytesSpilled"),
        "disk_spill_bytes": stage.get("diskBytesSpilled"),
        "peak_execution_memory": stage.get("peakExecutionMemory"),
        "failure_reason": _truncate(stage.get("failureReason"), FAILURE_CHARS),
    }


def _compact_executor(executor: dict) -> dict:
    return {
        "id": executor["id"],
        "active": executor.get("isActive"),
        "cores": executor.get("totalCores"),
        "max_storage_memory": executor.get("maxMemory"),
        "task_time_ms": executor.get("totalDuration"),
        "gc_ms": executor.get("totalGCTime"),
        "failed_tasks": executor.get("failedTasks"),
        "completed_tasks": executor.get("completedTasks"),
        "peak_jvm_heap": executor.get("peakMemoryMetrics", {}).get("JVMHeapMemory"),
        "remove_reason": _truncate(executor.get("removeReason"), FAILURE_CHARS),
    }


def _allowed_conf(environment: dict) -> dict[str, str]:
    return {
        key: value
        for key, value in environment.get("sparkProperties", [])
        if key.startswith(CONF_PREFIXES) and REDACTED_MARKER not in value
    }


def _totals(jobs: list[dict], stages: list[dict]) -> dict[str, int]:
    submitted = [parse_time(j.get("submissionTime")) for j in jobs]
    completed = [parse_time(j.get("completionTime")) for j in jobs]
    submitted = [t for t in submitted if t]
    completed = [t for t in completed if t]
    wall_ms = (
        int((max(completed) - min(submitted)).total_seconds() * 1000)
        if submitted and completed
        else 0
    )

    def total(key: str) -> int:
        return int(sum(s.get(key) or 0 for s in stages))

    return {
        "wall_time_ms": wall_ms,
        "task_time_ms": total("executorRunTime"),
        "gc_ms": total("jvmGcTime"),
        "input_bytes": total("inputBytes"),
        "output_bytes": total("outputBytes"),
        "shuffle_read_bytes": total("shuffleReadBytes"),
        "shuffle_write_bytes": total("shuffleWriteBytes"),
        "spill_disk_bytes": total("diskBytesSpilled"),
        "spill_memory_bytes": total("memoryBytesSpilled"),
        "tasks": total("numTasks"),
        "failed_tasks": total("numFailedTasks"),
        "stages": len(stages),
        "jobs": len(jobs),
        "failed_jobs": sum(1 for j in jobs if j.get("status") == "FAILED"),
    }


def collect_iteration(client: Any, after_job_id: int) -> IterationSnapshot | None:
    """
    The jobs that finished after `after_job_id`, or None if there are none.
    `client` only needs `app_id` and `get(path, **params)` (SparkRestClient).
    """
    all_jobs = client.get("/jobs")
    new_jobs = [j for j in all_jobs if j["jobId"] > after_job_id]
    jobs = sorted(
        (j for j in new_jobs if j.get("status") in FINISHED_JOB_STATES),
        key=lambda j: j["jobId"],
    )
    running = sorted(
        j["jobId"] for j in new_jobs if j.get("status") not in FINISHED_JOB_STATES
    )
    if not jobs:
        return None
    job_ids = [j["jobId"] for j in jobs]
    stage_ids = {sid for j in jobs for sid in j.get("stageIds", [])}

    # Skipped stages are the ones whose shuffle output an earlier job left
    # behind: listed under the job, but they ran nothing in this iteration.
    stages = [
        s
        for s in client.get("/stages")
        if s["stageId"] in stage_ids and s.get("status") != "SKIPPED"
    ]
    compact_stages = [_compact_stage(s) for s in stages]
    by_time = sorted(compact_stages, key=lambda s: s["task_time_ms"] or 0, reverse=True)
    for stage in by_time[:TOP_STAGES]:
        summary = client.get(
            f"/stages/{stage['stage_id']}/{stage['attempt_id']}/taskSummary",
            quantiles=TASK_QUANTILES,
        )
        stage["task_distribution"] = _task_distribution(summary)

    job_id_set = set(job_ids)
    sql = []
    for execution in client.get(
        "/sql", details="false", planDescription="true", length=1000
    ):
        execution_jobs = set(execution.get("successJobIds", [])) | set(
            execution.get("failedJobIds", [])
        )
        if execution_jobs & job_id_set:
            sql.append(
                {
                    "execution_id": execution["id"],
                    "description": execution.get("description"),
                    "status": execution.get("status"),
                    "duration_ms": execution.get("duration"),
                    "job_ids": sorted(execution_jobs),
                    "physical_plan": _truncate(
                        execution.get("planDescription"), SQL_PLAN_CHARS
                    ),
                }
            )

    executors = [_compact_executor(e) for e in client.get("/allexecutors")]
    environment = client.get("/environment")
    properties = dict(environment.get("sparkProperties", []))

    return IterationSnapshot(
        app_id=client.app_id,
        app_name=properties.get("spark.app.name", ""),
        job_ids=job_ids,
        jobs=[
            {
                "job_id": j["jobId"],
                "name": j.get("name"),
                "description": j.get("description"),
                "status": j.get("status"),
                "duration_ms": _ms_between(
                    j.get("submissionTime"), j.get("completionTime")
                ),
                "stage_ids": j.get("stageIds"),
                "skipped_stages": j.get("numSkippedStages"),
                "failed_tasks": j.get("numFailedTasks"),
            }
            for j in jobs
        ],
        stages=compact_stages,
        sql=sql,
        executors=executors[:EXECUTOR_LIMIT],
        conf=_allowed_conf(environment),
        totals=_totals(jobs, stages),
        running_job_ids=running,
    )
