import asyncio
import copy
import json
import os
import re
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, UserPromptPart
from pydantic_ai.models.function import FunctionModel

from orchestera.notebook import job_analysis
from orchestera.notebook.job_analysis import (
    Iteration,
    analyze_spark_jobs,
    metrics_table,
    reset_job_analysis,
)
from orchestera.notebook.llm import NotebookModel, NotebookModelError, notebook_model
from orchestera.notebook.spark_rest import (
    SparkRestClient,
    SparkRestError,
    collect_iteration,
)

# Captured from a local Spark 3.5.6 driver: SQL execution 0 is a skewed
# aggregation (jobs 0-1), execution 1 a broadcast join (jobs 2-4).
FIXTURES = Path(__file__).parent / "fixtures" / "spark_rest"


def fixture(name):
    return json.loads((FIXTURES / f"{name}.json").read_text())


class FakeSparkRest:
    """The driver's /api/v1/applications/<app> paths, served from fixtures."""

    def __init__(self, max_job_id=None, app_id="spark-test"):
        self.app_id = app_id
        self.jobs = [
            j for j in fixture("jobs") if max_job_id is None or j["jobId"] <= max_job_id
        ]
        self.calls = []

    def get(self, path, **params):
        self.calls.append((path, params))
        if path == "/jobs":
            return copy.deepcopy(self.jobs)
        if path == "/allexecutors":
            return fixture("executors")
        if re.fullmatch(r"/stages/\d+/\d+/taskSummary", path):
            return fixture("task_summary")
        if re.fullmatch(r"/stages/\d+/\d+/taskList", path):
            return fixture("task_list")
        if re.fullmatch(r"/sql/\d+", path):
            execution_id = int(path.rsplit("/", 1)[1])
            return next(e for e in fixture("sql") if e["id"] == execution_id)
        return fixture(path.strip("/"))


def scripted_model(answers, tool_calls=None):
    """
    A FunctionModel answering each run with the next of `answers`, after
    making `tool_calls` first. Records each run's user prompt.
    """
    prompts = []
    queue = list(answers)

    def respond(messages, info):
        last = messages[-1]
        user = [p for p in last.parts if isinstance(p, UserPromptPart)]
        if user:
            prompts.append(user[0].content)
            if tool_calls:
                return ModelResponse(
                    parts=[ToolCallPart(name, args) for name, args in tool_calls]
                )
        return ModelResponse(parts=[TextPart(queue.pop(0))])

    model = NotebookModel(
        model=FunctionModel(respond), settings=None, name="test/model"
    )
    return model, prompts


class CollectIterationTests(unittest.TestCase):
    def test_first_iteration_covers_every_finished_job(self):
        snapshot = collect_iteration(FakeSparkRest(), after_job_id=-1)

        self.assertEqual(snapshot.job_ids, [0, 1, 2, 3, 4])
        self.assertEqual(snapshot.app_name, "analysis-fixture")
        self.assertEqual([e["execution_id"] for e in snapshot.sql], [0, 1])
        self.assertIn("HashAggregate", snapshot.sql[0]["physical_plan"])
        # Job 4 re-used job 3's shuffle output: its stage 5 is SKIPPED.
        stage_ids = {s["stage_id"] for s in snapshot.stages}
        self.assertNotIn(5, stage_ids)
        stages = {s["stageId"]: s for s in fixture("stages")}
        self.assertEqual(
            snapshot.totals["task_time_ms"],
            sum(stages[i]["executorRunTime"] for i in stage_ids),
        )
        self.assertEqual(snapshot.totals["jobs"], 5)
        self.assertEqual(snapshot.totals["failed_jobs"], 0)
        self.assertGreater(snapshot.totals["wall_time_ms"], 0)

    def test_slowest_stages_get_a_task_distribution(self):
        snapshot = collect_iteration(FakeSparkRest(), after_job_id=-1)

        slowest = max(snapshot.stages, key=lambda s: s["task_time_ms"])
        distribution = slowest["task_distribution"]
        self.assertEqual(distribution["quantiles"], [0.0, 0.25, 0.5, 0.75, 1.0])
        self.assertEqual(distribution["run_time_skew_max_over_median"], 1.0)

    def test_later_iteration_covers_only_newer_jobs(self):
        snapshot = collect_iteration(FakeSparkRest(), after_job_id=1)

        self.assertEqual(snapshot.job_ids, [2, 3, 4])
        self.assertEqual([e["execution_id"] for e in snapshot.sql], [1])

    def test_no_new_finished_jobs_is_none(self):
        self.assertIsNone(collect_iteration(FakeSparkRest(), after_job_id=4))

    def test_running_jobs_wait_for_the_next_iteration(self):
        rest = FakeSparkRest()
        rest.jobs[0]["status"] = "RUNNING"  # job 4

        snapshot = collect_iteration(rest, after_job_id=1)

        self.assertEqual(snapshot.job_ids, [2, 3])
        self.assertEqual(snapshot.running_job_ids, [4])

    def test_only_allowlisted_unredacted_conf_reaches_the_model(self):
        snapshot = collect_iteration(FakeSparkRest(), after_job_id=-1)

        self.assertEqual(
            snapshot.conf,
            {"spark.sql.shuffle.partitions": "8", "spark.sql.adaptive.enabled": "true"},
        )
        self.assertNotIn("redacted", json.dumps(snapshot.for_prompt()))


class SparkRestClientTests(unittest.TestCase):
    def spark(self, ui_url="http://10.1.2.3:4041"):
        return SimpleNamespace(
            sparkContext=SimpleNamespace(uiWebUrl=ui_url, applicationId="spark-abc")
        )

    def test_prefers_loopback_on_the_ui_port(self):
        with patch.object(SparkRestClient, "get", return_value={}) as get:
            client = SparkRestClient.from_spark(self.spark())

        self.assertEqual(
            client.base, "http://127.0.0.1:4041/api/v1/applications/spark-abc"
        )
        get.assert_called_once_with("/environment")

    def test_falls_back_to_the_ui_url(self):
        with patch.object(
            SparkRestClient, "get", side_effect=[SparkRestError("refused"), {}]
        ):
            client = SparkRestClient.from_spark(self.spark())

        self.assertEqual(
            client.base, "http://10.1.2.3:4041/api/v1/applications/spark-abc"
        )

    def test_disabled_ui(self):
        with self.assertRaisesRegex(SparkRestError, "spark.ui.enabled"):
            SparkRestClient.from_spark(self.spark(ui_url=None))


class AnalyzeSparkJobsTests(unittest.TestCase):
    def setUp(self):
        reset_job_analysis()
        self.addCleanup(reset_job_analysis)

    def test_unclicked_button_does_nothing(self):
        rest = FakeSparkRest()

        result = analyze_spark_jobs(run=SimpleNamespace(value=False), client=rest)

        self.assertIn("click the analysis button", result.markdown)
        self.assertEqual(rest.calls, [])

    def test_each_click_analyses_the_new_jobs_against_the_previous_iteration(self):
        model, prompts = scripted_model(["### Verdict\nFirst.", "### Verdict\nSecond."])
        rest = FakeSparkRest(max_job_id=1)

        first = analyze_spark_jobs(
            run=SimpleNamespace(value=True), client=rest, model=model
        )
        rest.jobs = fixture("jobs")  # the user re-ran their code
        second = analyze_spark_jobs(client=rest, model=model)

        self.assertIsNone(first.error)
        self.assertIn("**Spark iteration 1**", first.markdown)
        self.assertIn("jobs 0–1", first.markdown)
        self.assertIn("First.", first.markdown)
        self.assertNotIn("Change", first.markdown)
        self.assertIn("There is no previous iteration", prompts[0])

        self.assertIn("jobs 2–4 · compared with iteration 1", second.markdown)
        self.assertIn("| | Iteration 1 | Iteration 2 | Change |", second.markdown)
        self.assertIn("Second.", second.markdown)
        self.assertIn('"job_ids": [2, 3, 4]', prompts[1])
        self.assertIn(
            "## Your analysis of iteration 1\n\n### Verdict\nFirst.", prompts[1]
        )

    def test_no_new_jobs_since_the_last_analysis(self):
        model, _ = scripted_model(["done"])
        rest = FakeSparkRest()
        analyze_spark_jobs(client=rest, model=model)

        result = analyze_spark_jobs(client=rest, model=model)

        self.assertEqual(result.error, "no_jobs")
        self.assertIn("since job 4", result.markdown)

    def test_scope_all_reanalyses_the_whole_session(self):
        model, prompts = scripted_model(["one", "two"])
        rest = FakeSparkRest()
        analyze_spark_jobs(client=rest, model=model)

        analyze_spark_jobs(client=rest, model=model, scope="all")

        self.assertIn('"job_ids": [0, 1, 2, 3, 4]', prompts[1])

    def test_model_reads_task_detail_through_the_tools(self):
        model, _ = scripted_model(
            ["### Verdict\nStage 1 is fine."],
            tool_calls=[
                ("get_stage_tasks", {"stage_id": 1, "limit": 500}),
                ("get_sql_plan", {"execution_id": 0}),
            ],
        )
        rest = FakeSparkRest()

        result = analyze_spark_jobs(client=rest, model=model, question="Why stage 1?")

        self.assertIn("Stage 1 is fine.", result.markdown)
        self.assertIn(
            ("/stages/1/0/taskList", {"sortBy": "-runtime", "length": 50}), rest.calls
        )
        self.assertIn(
            ("/sql/0", {"details": "true", "planDescription": "true"}), rest.calls
        )

    def test_missing_model_shows_metrics_and_keeps_the_jobs_for_the_next_click(self):
        rest = FakeSparkRest()
        with patch.object(
            job_analysis,
            "notebook_model",
            side_effect=NotebookModelError("Anthropic API key not configured."),
        ):
            failed = analyze_spark_jobs(client=rest)

        self.assertEqual(failed.error, "model")
        self.assertIn("| Task time |", failed.markdown)
        self.assertIn("Settings → Notebook AI assistant", failed.markdown)

        model, prompts = scripted_model(["ok"])
        retried = analyze_spark_jobs(client=rest, model=model)
        self.assertIn("**Spark iteration 1**", retried.markdown)
        self.assertIn('"job_ids": [0, 1, 2, 3, 4]', prompts[0])

    def test_runs_inside_an_event_loop_like_marimo_s_kernel(self):
        model, _ = scripted_model(["from a loop"])

        async def cell():
            return analyze_spark_jobs(client=FakeSparkRest(), model=model)

        self.assertIn("from a loop", asyncio.run(cell()).markdown)


class MetricsTableTests(unittest.TestCase):
    def iteration(self, number, **totals):
        base = dict.fromkeys([key for _, key, _ in job_analysis._TABLE_ROWS], 0)
        base.update(totals)
        return Iteration(number=number, snapshot=SimpleNamespace(totals=base))

    def test_changes_are_relative_to_the_previous_iteration(self):
        table = metrics_table(
            self.iteration(2, wall_time_ms=90_000, shuffle_read_bytes=3 * 1024**3),
            self.iteration(1, wall_time_ms=120_000, shuffle_read_bytes=0),
        )

        self.assertIn("| Wall time | 2.0 min | 1.5 min | -25% |", table)
        self.assertIn("| Shuffle read | 0 B | 3.0 GiB | new |", table)
        self.assertIn("| Failed tasks | 0 | 0 | = |", table)


class NotebookModelTests(unittest.TestCase):
    """Against a marimo.toml as the control plane writes it (notebook_ai.py)."""

    MARIMO_TOML = """\
[ai]
mode = "ask"

[ai.models]
chat_model = "anthropic/claude-sonnet-4-5"

[ai.anthropic]
api_key = "env:ANTHROPIC_API_KEY"
"""

    def setUp(self):
        from marimo._config.utils import get_user_config_path

        home = tempfile.TemporaryDirectory()
        self.addCleanup(home.cleanup)
        config_dir = Path(home.name, ".config", "marimo")
        config_dir.mkdir(parents=True)
        (config_dir / "marimo.toml").write_text(self.MARIMO_TOML)
        cwd = os.getcwd()
        os.chdir(home.name)
        self.addCleanup(os.chdir, cwd)
        env = patch.dict(
            os.environ,
            {"HOME": home.name, "XDG_CONFIG_HOME": str(Path(home.name, ".config"))},
        )
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("ANTHROPIC_API_KEY", None)
        get_user_config_path.cache_clear()
        self.addCleanup(get_user_config_path.cache_clear)

    def test_uses_the_chat_panel_model_and_key(self):
        from pydantic_ai.models.anthropic import AnthropicModel

        os.environ["ANTHROPIC_API_KEY"] = "sk-ant-test-key"

        resolved = notebook_model()

        self.assertEqual(resolved.name, "anthropic/claude-sonnet-4-5")
        self.assertIsInstance(resolved.model, AnthropicModel)
        self.assertEqual(resolved.model.model_name, "claude-sonnet-4-5")

    def test_missing_key_names_the_variable(self):
        with self.assertRaisesRegex(NotebookModelError, "ANTHROPIC_API_KEY"):
            notebook_model()


if __name__ == "__main__":
    unittest.main()
