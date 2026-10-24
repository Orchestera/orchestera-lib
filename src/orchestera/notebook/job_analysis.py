"""
One-click analysis of the Spark jobs a notebook just ran, in a cell.

    analyze = job_analysis_button()
    analyze
    ---
    analyze_spark_jobs(spark, run=analyze)

Each click analyses the jobs that finished since the previous click (one
"iteration" of the user's code), compares them with the previous iteration
and asks the notebook's model (`llm.notebook_model`) what to change. The
previous analysis goes along, so the model can tell whether its advice was
applied and whether it helped.

The button keeps the model call explicit: Marimo re-runs a cell whenever
something it reads changes, and a re-run of the session cell must not cost
an LLM call. A run_button is only true for the run its click started.

Iterations live in this process, so a kernel restart starts over.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
from dataclasses import dataclass, field
from typing import Any

from orchestera.notebook.llm import NotebookModel, NotebookModelError, notebook_model
from orchestera.notebook.spark_rest import (
    IterationSnapshot,
    SparkRestClient,
    SparkRestError,
    collect_iteration,
)

BUTTON_LABEL = "Analyze Spark jobs"
PREVIOUS_ANALYSIS_CHARS = 6000
TOOL_RESULT_CHARS = 20000
TOOL_TASK_LIMIT = 50

INSTRUCTIONS = """\
You are a Spark performance engineer reviewing one iteration of a user's \
PySpark notebook code. You get the metrics of the Spark jobs this iteration \
ran (read from the live driver), and usually the previous iteration's metrics \
and your previous analysis.

Write GitHub-flavoured markdown with these sections, and keep it tight:
### Verdict
One or two sentences: is this iteration healthy, and the single biggest issue.
### Bottlenecks
Ranked. Cite the job, stage and SQL execution ids and the metric behind each \
claim (task-time share, skew max/median, spill, shuffle volume, GC, failures, \
executor loss). Explain the cause from the physical plan where it shows one \
(join strategy, exchange, scan, AQE decisions).
### Since the last iteration
Only when there is a previous iteration: what changed in the plan or metrics, \
whether your previous recommendations were applied, and whether they helped. \
Different code can make totals incomparable; say so instead of forcing a diff.
### Next changes
Concrete, ordered by expected impact: the PySpark code change or the Spark \
conf with a value, and why. Prefer code changes over bigger hardware.

Use the tools for task-level detail (a skewed or failed stage, a full plan) \
before guessing. Never invent ids or numbers. Tiny local test data makes \
timings noise; say so rather than over-tuning. Durations are milliseconds and \
sizes are bytes in the data; write them human-readable."""


@dataclass
class Iteration:
    number: int
    snapshot: IterationSnapshot
    analysis: str | None = None


@dataclass
class _History:
    iterations: list[Iteration] = field(default_factory=list)
    # Highest job id analysed, per application.
    last_job_id: dict[str, int] = field(default_factory=dict)


_history = _History()


def reset_job_analysis() -> None:
    """Forget earlier iterations: the next click analyses the whole session."""
    global _history
    _history = _History()


@dataclass
class JobAnalysis:
    """A cell's output. Marimo renders `_display_`; `markdown` has the text."""

    markdown: str
    iteration: Iteration | None = None
    error: str | None = None

    def _display_(self) -> Any:
        import marimo as mo

        return mo.md(self.markdown)

    def _repr_markdown_(self) -> str:
        return self.markdown

    def __str__(self) -> str:
        return self.markdown


def job_analysis_button(label: str = BUTTON_LABEL) -> Any:
    import marimo as mo

    return mo.ui.run_button(label=label)


def _format_bytes(value: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024 or unit == "TiB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def _format_ms(value: float) -> str:
    if abs(value) < 1000:
        return f"{value:.0f} ms"
    if abs(value) < 60_000:
        return f"{value / 1000:.1f} s"
    return f"{value / 60_000:.1f} min"


# (label, totals key, formatter); the comparison table's rows.
_TABLE_ROWS = (
    ("Wall time", "wall_time_ms", _format_ms),
    ("Task time", "task_time_ms", _format_ms),
    ("GC time", "gc_ms", _format_ms),
    ("Input", "input_bytes", _format_bytes),
    ("Shuffle read", "shuffle_read_bytes", _format_bytes),
    ("Shuffle write", "shuffle_write_bytes", _format_bytes),
    ("Spill (disk)", "spill_disk_bytes", _format_bytes),
    ("Tasks", "tasks", str),
    ("Failed tasks", "failed_tasks", str),
    ("Failed jobs", "failed_jobs", str),
)


def _delta(current: int, previous: int) -> str:
    if current == previous:
        return "="
    if not previous:
        return "new"
    change = (current - previous) / previous * 100
    return f"{change:+.0f}%"


def _job_range(job_ids: list[int]) -> str:
    if len(job_ids) == 1:
        return f"job {job_ids[0]}"
    return f"jobs {job_ids[0]}–{job_ids[-1]}"


def metrics_table(current: Iteration, previous: Iteration | None) -> str:
    totals = current.snapshot.totals
    if previous is None:
        lines = [f"| | Iteration {current.number} |", "|---|---:|"]
        for label, key, fmt in _TABLE_ROWS:
            lines.append(f"| {label} | {fmt(totals[key])} |")
        return "\n".join(lines)
    before = previous.snapshot.totals
    lines = [
        f"| | Iteration {previous.number} | Iteration {current.number} | Change |",
        "|---|---:|---:|---:|",
    ]
    for label, key, fmt in _TABLE_ROWS:
        lines.append(
            f"| {label} | {fmt(before[key])} | {fmt(totals[key])} "
            f"| {_delta(totals[key], before[key])} |"
        )
    return "\n".join(lines)


def _header(current: Iteration, previous: Iteration | None) -> str:
    snapshot = current.snapshot
    parts = [
        f"**Spark iteration {current.number}**",
        f"`{snapshot.app_id}`",
        _job_range(snapshot.job_ids),
    ]
    if previous is not None:
        parts.append(f"compared with iteration {previous.number}")
    header = "## " + " · ".join(parts)
    if snapshot.running_job_ids:
        header += (
            f"\n\n_Still running, left for the next analysis: "
            f"{_job_range(snapshot.running_job_ids)}._"
        )
    return header


def _prompt(
    current: Iteration, previous: Iteration | None, question: str | None
) -> str:
    sections = [
        f"## Iteration {current.number} (this one)",
        json.dumps(current.snapshot.for_prompt(), default=str),
    ]
    if previous is None:
        sections.append("There is no previous iteration in this session.")
    else:
        sections += [
            f"## Iteration {previous.number} (previous)",
            json.dumps(previous.snapshot.for_prompt(), default=str),
        ]
        if previous.analysis:
            sections += [
                f"## Your analysis of iteration {previous.number}",
                previous.analysis[:PREVIOUS_ANALYSIS_CHARS],
            ]
    if question:
        sections += ["## The user's question", question]
    return "\n\n".join(sections)


def _tool_result(data: Any) -> str:
    text = json.dumps(data, default=str)
    if len(text) > TOOL_RESULT_CHARS:
        return text[:TOOL_RESULT_CHARS] + " ... [truncated]"
    return text


def _build_agent(client: SparkRestClient, model: NotebookModel) -> Any:
    import pydantic_ai
    from pydantic_ai import Agent

    # Its first-run banner would land in the cell's console output.
    pydantic_ai.BANNER_ENABLED = False
    agent = Agent(
        model.model,
        instructions=INSTRUCTIONS,
        model_settings=model.settings,
        name="orchestera-job-analysis",
    )

    # Every tool reads the same live application; the model can't point
    # them anywhere else.
    @agent.tool_plain
    def get_stage_tasks(
        stage_id: int, attempt_id: int = 0, sort_by: str = "-runtime", limit: int = 20
    ) -> str:
        """
        Tasks of one stage attempt, sorted. sort_by: -runtime (slowest first),
        runtime, ID, or increasingRuntime/decreasingRuntime. limit <= 50.
        """
        tasks = client.get(
            f"/stages/{stage_id}/{attempt_id}/taskList",
            sortBy=sort_by,
            length=max(1, min(limit, TOOL_TASK_LIMIT)),
        )
        for task in tasks:
            task.pop("accumulatorUpdates", None)
            task.pop("executorLogs", None)
        return _tool_result(tasks)

    @agent.tool_plain
    def get_stage_task_summary(stage_id: int, attempt_id: int = 0) -> str:
        """Task metric quantiles (min, 25%, median, 75%, max) of one stage attempt."""
        return _tool_result(
            client.get(
                f"/stages/{stage_id}/{attempt_id}/taskSummary",
                quantiles="0.0,0.25,0.5,0.75,1.0",
            )
        )

    @agent.tool_plain
    def get_sql_plan(execution_id: int) -> str:
        """The full physical plan and per-operator metrics of one SQL execution."""
        execution = client.get(
            f"/sql/{execution_id}", details="true", planDescription="true"
        )
        return _tool_result(execution)

    return agent


def _run_blocking(make_coroutine: Any) -> Any:
    # Marimo may already run an event loop on the kernel thread; a fresh
    # loop on a worker thread works either way.
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(make_coroutine())
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(make_coroutine())).result()


def _spinner(title: str) -> Any:
    try:
        import marimo as mo

        if mo.running_in_notebook():
            return mo.status.spinner(title=title)
    except ImportError:
        pass
    import contextlib

    return contextlib.nullcontext()


def analyze_spark_jobs(
    spark: Any = None,
    *,
    run: Any = None,
    question: str | None = None,
    scope: str = "new",
    model: str | NotebookModel | None = None,
    client: Any = None,
) -> JobAnalysis:
    """
    Analyse the Spark jobs that finished since the previous call.

    run: a `job_analysis_button()`; nothing happens until it is clicked.
    question: anything specific to ask about this iteration.
    scope: "new" (since the last analysis) or "all" (the whole session).
    model: a Marimo model id; default is the chat panel's model.
    """
    if run is not None and not getattr(run, "value", False):
        return JobAnalysis(
            "_Run your Spark code, then click the analysis button to analyse "
            "the jobs it ran._"
        )
    if scope not in ("new", "all"):
        raise ValueError('scope must be "new" or "all"')

    try:
        if client is None:
            if spark is None:
                from pyspark.sql import SparkSession

                spark = SparkSession.getActiveSession()
            if spark is None:
                return JobAnalysis(
                    "No active Spark session. Start one with "
                    "`OrchesteraSparkSession` first.",
                    error="no_session",
                )
            client = SparkRestClient.from_spark(spark)
        after = -1 if scope == "all" else _history.last_job_id.get(client.app_id, -1)
        snapshot = collect_iteration(client, after_job_id=after)
    except SparkRestError as exc:
        return JobAnalysis(f"**Couldn't read the Spark metrics.** {exc}", error="rest")

    if snapshot is None:
        since = "" if after < 0 else f" since job {after}"
        return JobAnalysis(
            f"No Spark jobs finished{since} in `{client.app_id}`. "
            f"Run your Spark code, then analyse again.",
            error="no_jobs",
        )

    previous = _history.iterations[-1] if _history.iterations else None
    current = Iteration(number=len(_history.iterations) + 1, snapshot=snapshot)

    head = f"{_header(current, previous)}\n\n{metrics_table(current, previous)}"
    try:
        resolved = model if isinstance(model, NotebookModel) else notebook_model(model)
        agent = _build_agent(client, resolved)
        prompt = _prompt(current, previous, question)
        with _spinner(f"Analysing {_job_range(snapshot.job_ids)} with {resolved.name}"):
            result = _run_blocking(lambda: agent.run(prompt))
    except NotebookModelError as exc:
        return JobAnalysis(
            f"{head}\n\n**No model to analyse with.** {exc}\n\n"
            "Add a key under **Settings → Notebook AI assistant** in Orchestera "
            "and relaunch the notebook, or set one in Marimo's AI settings.",
            iteration=current,
            error="model",
        )
    except Exception as exc:  # the provider's own errors: auth, quota, network
        return JobAnalysis(
            f"{head}\n\n**The analysis failed:** {type(exc).__name__}: {exc}",
            iteration=current,
            error="llm",
        )

    # Only an analysed iteration counts: after a missing key or a provider
    # error, the next click covers the same jobs again.
    current.analysis = str(result.output)
    _history.iterations.append(current)
    _history.last_job_id[client.app_id] = max(
        _history.last_job_id.get(client.app_id, -1), snapshot.job_ids[-1]
    )
    footer = (
        f"\n\n<sub>{resolved.name} · re-run your code and click again to compare.</sub>"
    )
    return JobAnalysis(f"{head}\n\n{current.analysis}{footer}", iteration=current)
