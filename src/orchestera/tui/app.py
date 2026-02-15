import json
import subprocess
import threading
from difflib import get_close_matches

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.events import Key
from textual.widgets import Input, Static, TextArea


class OrchesteraTuiApp(App):
    ASCII_ART = r"""
   ____            __               __
  / __ \__________/ /_  ___  _____/ /____  _________ _
 / / / / ___/ ___/ __ \/ _ \/ ___/ __/ _ \/ ___/ __ `/
/ /_/ / /  / /__/ / / /  __(__  ) /_/  __/ /  / /_/ /
\____/_/   \___/_/ /_/\___/____/\__/\___/_/   \__,_/
""".strip(
        "\n"
    )

    CSS = """
    Screen {
        layout: vertical;
        background: #0b1220;
        color: #e2e8f0;
    }

    #banner {
        padding: 0 1;
        color: #22d3ee;
        background: #111827;
    }

    #status {
        height: 1;
        padding: 0 1;
        color: #f8fafc;
        background: #1f2937;
    }

    #links {
        padding: 0 1;
        height: 6;
        color: #86efac;
        background: #0f172a;
        border: solid #334155;
    }

    #command-docs {
        padding: 0 1;
        height: 14;
        border: solid #f59e0b;
        background: #111827;
        color: #cbd5e1;
    }

    #command-input {
        height: 3;
        border: solid #22c55e;
        background: #0f172a;
        color: #f8fafc;
    }

    #suggestion {
        padding: 0 1;
        height: 1;
        color: #fde68a;
    }

    #json-editor {
        height: 1fr;
        border: solid #f59e0b;
        background: #0f172a;
        color: #f8fafc;
    }

    #json-editor-help {
        height: 2;
        padding: 0 1;
        color: #fcd34d;
        background: #1f2937;
    }

    .hidden {
        display: none;
    }
    """

    BINDINGS = [
        Binding("ctrl+c", "quit", "Quit"),
        Binding(
            "tab", "autocomplete_command", "Autocomplete", show=False, priority=True
        ),
    ]

    COMMANDS = {
        "/start-proxy": "_start_proxy",
        "/stop-proxy": "_stop_proxy",
        "/update-cluster-context": "_update_cluster_context",
        "/upload-iam-policy": "_upload_iam_policy",
        "/upload-secrets": "_upload_secrets",
        "/exit": "_exit_app",
    }

    def __init__(self) -> None:
        super().__init__()
        self._proxy_process: subprocess.Popen[str] | None = None
        self._proxy_links_announced = False
        self._json_editor_active = False
        self._json_editor_mode: str | None = None
        self._json_editor_cluster_name: str | None = None
        self._json_editor_namespace: str | None = None
        self._json_editor_alias: str | None = None
        self._command_history: list[str] = []
        self._history_index: int | None = None
        self._history_draft: str = ""

    def compose(self) -> ComposeResult:
        yield Static(self.ASCII_ART, id="banner")
        yield Static("Proxy status: stopped", id="status")
        yield Static(self._proxy_links_text(), id="links", markup=True)
        yield Static(self._command_docs_text(), id="command-docs", markup=True)
        yield TextArea("", id="json-editor", classes="hidden")
        yield Static("", id="json-editor-help", classes="hidden")
        yield Input(
            placeholder="Type a command and press Enter (Tab to autocomplete)",
            id="command-input",
        )
        yield Static(self._default_suggestion_text(), id="suggestion")

    def on_mount(self) -> None:
        self.title = "Orchestera TUI"
        self.notify("Ready. Run /start-proxy to start proxy port-forward.", timeout=4)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if self._json_editor_active:
            self.notify(
                "JSON editor is active. Press Ctrl+S to submit or Esc to cancel.",
                timeout=4,
            )
            return

        command = event.value.strip()
        event.input.value = ""
        self._history_index = None
        self._history_draft = ""
        self._set_suggestion(self._default_suggestion_text())
        if not command:
            return
        self._record_command_history(command)

        command_name, args = self._parse_command(command)
        if not command_name:
            return

        resolved_command = self._resolve_command(command_name)
        if resolved_command is None:
            self.notify(
                "Unknown command. Use /start-proxy, /stop-proxy, /update-cluster-context, /upload-iam-policy, /upload-secrets, or /exit."
            )
            return

        if resolved_command != command_name:
            self.notify(f"Using closest match: {resolved_command}", timeout=2)

        handler_name = self.COMMANDS[resolved_command]
        getattr(self, handler_name)(args)

    def on_input_changed(self, event: Input.Changed) -> None:
        if self._json_editor_active:
            return

        text = event.value.strip()
        if not text:
            self._set_suggestion(self._default_suggestion_text())
            return

        command_name, _ = self._parse_command(text)
        if not command_name:
            self._set_suggestion(self._default_suggestion_text())
            return

        if command_name in self.COMMANDS:
            self._set_suggestion(f"Selected: {command_name}")
            return

        match = self._best_command_match(command_name)
        if match is None:
            self._set_suggestion("No close command match.")
            return

        self._set_suggestion(f"Did you mean: {match} (Tab)")

    def on_key(self, event: Key) -> None:
        if self._json_editor_active:
            if event.key == "ctrl+s":
                event.prevent_default()
                event.stop()
                self._submit_json_editor()
                return
            if event.key == "escape":
                event.prevent_default()
                event.stop()
                self._cancel_json_editor()
                return

        if self.focused is self.query_one("#command-input", Input):
            if event.key == "up":
                event.prevent_default()
                event.stop()
                self._show_previous_command()
                return
            if event.key == "down":
                event.prevent_default()
                event.stop()
                self._show_next_command()
                return

        if event.key != "tab":
            return
        if self.focused is not self.query_one("#command-input", Input):
            return

        event.prevent_default()
        event.stop()
        self.action_autocomplete_command()

    def action_quit(self) -> None:
        self._exit_app()

    def action_autocomplete_command(self) -> None:
        if self._json_editor_active:
            return

        input_widget = self.query_one("#command-input", Input)
        current = input_widget.value.strip()
        if not current:
            return

        command_name, args = self._parse_command(current)
        if not command_name:
            return

        match = self._best_command_match(command_name)
        if match is None:
            return

        completed = f"{match} {' '.join(args)}".rstrip()
        input_widget.value = completed
        input_widget.cursor_position = len(completed)
        self._set_suggestion(f"Selected: {match}")

    def _exit_app(self, _args: list[str] | None = None) -> None:
        self._stop_proxy()
        self.exit()

    def _start_proxy(self, _args: list[str] | None = None) -> None:
        if self._proxy_process is not None and self._proxy_process.poll() is None:
            self._set_status("running")
            self.notify("Proxy is already running.", timeout=2)
            return

        command = [
            "kubectl",
            "port-forward",
            "svc/traefik",
            "-n",
            "traefik",
            "8080:80",
            "3080:3080",
            "9001:8080",
        ]

        try:
            self._proxy_process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except OSError as exc:
            self._set_status("failed")
            self.notify(f"Failed to start proxy: {exc}", severity="error", timeout=6)
            return

        self._set_status("starting")
        self._proxy_links_announced = False
        self._announce_proxy_links()

        threading.Thread(target=self._stream_proxy_output, daemon=True).start()
        threading.Thread(target=self._wait_for_proxy_exit, daemon=True).start()

    def _stream_proxy_output(self) -> None:
        process = self._proxy_process
        if process is None or process.stdout is None:
            return

        for raw_line in process.stdout:
            line = raw_line.rstrip()
            if not line:
                continue

            if "forwarding from" in line.lower():
                self.call_from_thread(self._set_status, "running")
                self.call_from_thread(self._announce_proxy_links)

    def _wait_for_proxy_exit(self) -> None:
        process = self._proxy_process
        if process is None:
            return

        exit_code = process.wait()
        # Ignore stale processes that were intentionally detached/stopped.
        if self._proxy_process is not process:
            return

        self._proxy_process = None
        status = "stopped" if exit_code == 0 else "failed"
        self.call_from_thread(self._set_status, status)

    def _stop_proxy(self, _args: list[str] | None = None) -> None:
        process = self._proxy_process
        if process is None or process.poll() is not None:
            self._set_status("stopped")
            return

        # Detach first so wait thread won't report this intentional termination as failure.
        self._proxy_process = None
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

        self._set_status("stopped")

    def _update_cluster_context(self, args: list[str] | None = None) -> None:
        command_args = args or []
        if len(command_args) != 1:
            self.notify(
                "Usage: /update-cluster-context <cluster-name>",
                severity="warning",
                timeout=5,
            )
            return

        cluster_name = command_args[0]
        self.notify(f"Updating cluster context for {cluster_name}...", timeout=4)
        threading.Thread(
            target=self._run_update_cluster_context, args=(cluster_name,), daemon=True
        ).start()

    def _run_update_cluster_context(self, cluster_name: str) -> None:
        command = [
            "aws",
            "eks",
            "update-kubeconfig",
            "--name",
            cluster_name,
            "--region",
            "us-east-1",
            "--profile",
            "orchestera-dev",
        ]
        try:
            completed = subprocess.run(
                command, capture_output=True, text=True, check=False
            )
        except OSError as exc:
            self.call_from_thread(
                self.notify,
                f"Failed to run aws cli: {exc}",
                severity="error",
                timeout=6,
            )
            return

        if completed.returncode == 0:
            self.call_from_thread(
                self.notify,
                f"Cluster context updated for {cluster_name}.",
                timeout=5,
            )
            return

        error_output = (completed.stderr or completed.stdout or "").strip()
        if not error_output:
            error_output = f"exit code {completed.returncode}"
        self.call_from_thread(
            self.notify,
            f"Cluster context update failed: {error_output}",
            severity="error",
            timeout=8,
        )

    def _upload_iam_policy(self, args: list[str] | None = None) -> None:
        command_args = args or []
        if len(command_args) != 3:
            self.notify(
                "Usage: /upload-iam-policy <cluster-name> <namespace> <alias>",
                severity="warning",
                timeout=5,
            )
            return

        cluster_name, namespace, alias = command_args
        validation_error = self._validate_path_segments(
            cluster_name=cluster_name,
            namespace=namespace,
            alias=alias,
        )
        if validation_error:
            self.notify(validation_error, severity="warning", timeout=5)
            return

        self._enter_json_editor(
            mode="iam",
            cluster_name=cluster_name,
            namespace=namespace,
            alias=alias,
        )

    def _upload_secrets(self, args: list[str] | None = None) -> None:
        command_args = args or []
        if len(command_args) != 3:
            self.notify(
                "Usage: /upload-secrets <cluster-name> <namespace> <alias>",
                severity="warning",
                timeout=5,
            )
            return

        cluster_name, namespace, alias = command_args
        validation_error = self._validate_path_segments(
            cluster_name=cluster_name,
            namespace=namespace,
            alias=alias,
        )
        if validation_error:
            self.notify(validation_error, severity="warning", timeout=5)
            return

        self._enter_json_editor(
            mode="secrets",
            cluster_name=cluster_name,
            namespace=namespace,
            alias=alias,
        )

    def _enter_json_editor(
        self,
        mode: str,
        cluster_name: str,
        namespace: str,
        alias: str,
    ) -> None:
        if self._json_editor_active:
            self.notify(
                "JSON editor is already open. Submit with Ctrl+S or cancel with Esc.",
                timeout=4,
            )
            return

        self._json_editor_active = True
        self._json_editor_mode = mode
        self._json_editor_cluster_name = cluster_name
        self._json_editor_namespace = namespace
        self._json_editor_alias = alias

        parameter_name = self._build_ssm_parameter_name(
            mode=mode,
            cluster_name=cluster_name,
            namespace=namespace,
            alias=alias,
        )

        editor = self.query_one("#json-editor", TextArea)
        editor.text = "{}"
        editor.remove_class("hidden")

        editor_help = self.query_one("#json-editor-help", Static)
        editor_help.update(f"Editing {parameter_name}\nCtrl+S upload | Esc cancel")
        editor_help.remove_class("hidden")

        command_input = self.query_one("#command-input", Input)
        command_input.disabled = True

        self._set_suggestion("JSON editor active.")
        self.notify(f"Editing {parameter_name}", timeout=5)
        editor.focus()

    def _cancel_json_editor(self) -> None:
        if not self._json_editor_active:
            return
        self._close_json_editor()
        self.notify("JSON upload canceled.", timeout=3)

    def _submit_json_editor(self) -> None:
        if not self._json_editor_active:
            return

        mode = self._json_editor_mode
        cluster_name = self._json_editor_cluster_name
        namespace = self._json_editor_namespace
        alias = self._json_editor_alias
        if not mode or not cluster_name or not namespace or not alias:
            self._close_json_editor()
            self.notify(
                "Editor state is invalid. Please retry command.", severity="error"
            )
            return

        editor = self.query_one("#json-editor", TextArea)
        payload_text = editor.text.strip()
        validation_error = self._validate_json_payload(
            mode=mode, payload_text=payload_text
        )
        if validation_error is not None:
            self.notify(validation_error, severity="error", timeout=6)
            return

        parameter_name = self._build_ssm_parameter_name(
            mode=mode,
            cluster_name=cluster_name,
            namespace=namespace,
            alias=alias,
        )
        self._close_json_editor()
        self.notify(f"Uploading {parameter_name}...", timeout=4)
        threading.Thread(
            target=self._run_put_parameter,
            args=(mode, parameter_name, payload_text),
            daemon=True,
        ).start()

    def _close_json_editor(self) -> None:
        editor = self.query_one("#json-editor", TextArea)
        editor.add_class("hidden")
        editor.text = ""

        editor_help = self.query_one("#json-editor-help", Static)
        editor_help.add_class("hidden")
        editor_help.update("")

        command_input = self.query_one("#command-input", Input)
        command_input.disabled = False
        command_input.value = ""
        command_input.focus()

        self._json_editor_active = False
        self._json_editor_mode = None
        self._json_editor_cluster_name = None
        self._json_editor_namespace = None
        self._json_editor_alias = None
        self._set_suggestion(self._default_suggestion_text())

    def _run_put_parameter(
        self, mode: str, parameter_name: str, payload_text: str
    ) -> None:
        parameter_type = "String" if mode == "iam" else "SecureString"
        command = [
            "aws",
            "ssm",
            "put-parameter",
            "--name",
            parameter_name,
            "--type",
            parameter_type,
            "--value",
            payload_text,
            "--overwrite",
            "--region",
            "us-east-1",
            "--profile",
            "orchestera-useradmin",
        ]

        try:
            completed = subprocess.run(
                command, capture_output=True, text=True, check=False
            )
        except OSError as exc:
            self.call_from_thread(
                self.notify,
                f"Failed to run aws cli: {exc}",
                severity="error",
                timeout=6,
            )
            return

        if completed.returncode == 0:
            self.call_from_thread(
                self.notify,
                f"Uploaded parameter: {parameter_name}",
                timeout=6,
            )
            return

        error_output = (completed.stderr or completed.stdout or "").strip()
        if not error_output:
            error_output = f"exit code {completed.returncode}"
        self.call_from_thread(
            self.notify,
            f"Upload failed for {parameter_name}: {error_output}",
            severity="error",
            timeout=8,
        )

    def _set_status(self, status: str) -> None:
        self.query_one("#status", Static).update(f"Proxy status: {status}")

    def _set_suggestion(self, message: str) -> None:
        self.query_one("#suggestion", Static).update(message)

    def _commands_hint(self) -> str:
        return (
            "Commands: /start-proxy /stop-proxy /update-cluster-context "
            "/upload-iam-policy /upload-secrets /exit"
        )

    def _default_suggestion_text(self) -> str:
        return "Type a command. Tab autocomplete. Up/Down history."

    def _command_docs_text(self) -> str:
        return (
            "[bold #fbbf24]Commands[/bold #fbbf24]\n"
            "[bold #f59e0b]/start-proxy[/bold #f59e0b]\n"
            "[#cbd5e1]  Start proxy port-forward and show localhost links.[/#cbd5e1]\n"
            "[bold #f59e0b]/stop-proxy[/bold #f59e0b]\n"
            "[#cbd5e1]  Stop proxy port-forward.[/#cbd5e1]\n"
            "[bold #f59e0b]/update-cluster-context <cluster-name>[/bold #f59e0b]\n"
            "[#cbd5e1]  Run aws eks update-kubeconfig for the cluster.[/#cbd5e1]\n"
            "[bold #f59e0b]/upload-iam-policy <cluster-name> <namespace> <alias>[/bold #f59e0b]\n"
            "[#cbd5e1]  Open JSON editor and upload IAM policy to Parameter Store.[/#cbd5e1]\n"
            "[bold #f59e0b]/upload-secrets <cluster-name> <namespace> <alias>[/bold #f59e0b]\n"
            "[#cbd5e1]  Open JSON editor and upload secrets to Parameter Store.[/#cbd5e1]\n"
            "[bold #f59e0b]/exit[/bold #f59e0b]\n"
            "[#cbd5e1]  Exit the TUI.[/#cbd5e1]"
        )

    def _proxy_links_text(self) -> str:
        return (
            "[bold #86efac]Service Links[/bold #86efac]\n"
            "[#bbf7d0]JupyterHub: http://localhost:8080/jupyterhub[/#bbf7d0]\n"
            "[#bbf7d0]Spark History Server: http://localhost:8080/spark-history[/#bbf7d0]\n"
            "[#bbf7d0]AI Debugger: http://localhost:3080[/#bbf7d0]"
        )

    def _announce_proxy_links(self) -> None:
        if self._proxy_links_announced:
            return
        self._proxy_links_announced = True
        self.notify("JupyterHub: http://localhost:8080/jupyterhub", timeout=5)
        self.notify(
            "Spark History Server: http://localhost:8080/spark-history", timeout=5
        )
        self.notify("AI Debugger: http://localhost:3080", timeout=5)

    def _resolve_command(self, command: str) -> str | None:
        if command in self.COMMANDS:
            return command

        matches = get_close_matches(command, self.COMMANDS.keys(), n=1, cutoff=0.5)
        if matches:
            return matches[0]

        return None

    def _best_command_match(self, command: str) -> str | None:
        if command in self.COMMANDS:
            return command

        prefix_matches = [
            candidate for candidate in self.COMMANDS if candidate.startswith(command)
        ]
        if prefix_matches:
            return prefix_matches[0]

        fuzzy_matches = get_close_matches(
            command, self.COMMANDS.keys(), n=1, cutoff=0.35
        )
        if fuzzy_matches:
            return fuzzy_matches[0]

        return None

    def _parse_command(self, raw: str) -> tuple[str, list[str]]:
        parts = raw.split()
        if not parts:
            return "", []
        return parts[0], parts[1:]

    def _validate_path_segments(
        self, cluster_name: str, namespace: str, alias: str
    ) -> str | None:
        cluster_error = self._validate_path_segment("cluster-name", cluster_name)
        if cluster_error:
            return cluster_error

        namespace_error = self._validate_path_segment("namespace", namespace)
        if namespace_error:
            return namespace_error

        alias_error = self._validate_path_segment("alias", alias)
        if alias_error:
            return alias_error

        return None

    def _validate_path_segment(self, label: str, value: str) -> str | None:
        if not value:
            return f"{label} cannot be empty."
        if "/" in value:
            return f"{label} cannot contain '/'."
        if any(character.isspace() for character in value):
            return f"{label} cannot contain whitespace."
        return None

    def _build_ssm_parameter_name(
        self, mode: str, cluster_name: str, namespace: str, alias: str
    ) -> str:
        if mode == "iam":
            return f"/orchestera/sparklith/{cluster_name}/{namespace}/iams/{alias}"
        return f"/orchestera/sparklith/{cluster_name}/{namespace}/secrets/{alias}"

    def _validate_json_payload(self, mode: str, payload_text: str) -> str | None:
        if not payload_text:
            return "JSON payload cannot be empty."

        payload_size = len(payload_text.encode("utf-8"))
        if payload_size > 4096:
            return "JSON payload exceeds 4096 bytes (Parameter Store Standard limit)."

        try:
            parsed = json.loads(payload_text)
        except json.JSONDecodeError as exc:
            return f"Invalid JSON: {exc.msg} at line {exc.lineno}, column {exc.colno}."

        if not isinstance(parsed, dict):
            return "JSON payload must be an object."
        if not parsed:
            return "JSON payload object cannot be empty."

        if mode == "iam":
            return self._validate_iam_payload(parsed)

        return self._validate_secrets_payload(parsed)

    def _validate_iam_payload(self, payload: dict) -> str | None:
        for key in ("service_account_name", "role_name"):
            value = payload.get(key)
            if not isinstance(value, str) or not value.strip():
                return f'IAM payload requires non-empty string "{key}".'

        inline_policies = payload.get("inline_policies")
        if not isinstance(inline_policies, dict):
            return 'IAM payload requires object "inline_policies".'
        if not inline_policies:
            return '"inline_policies" cannot be empty.'
        return None

    def _validate_secrets_payload(self, payload: dict) -> str | None:
        for key, value in payload.items():
            if not isinstance(key, str) or not key.strip():
                return "Secrets payload keys must be non-empty strings."
            if not isinstance(value, str):
                return (
                    f'Secrets payload value for "{key}" must be a string '
                    "(flat JSON only)."
                )
        return None

    def _record_command_history(self, command: str) -> None:
        if self._command_history and self._command_history[-1] == command:
            return
        self._command_history.append(command)

    def _show_previous_command(self) -> None:
        if not self._command_history:
            return

        input_widget = self.query_one("#command-input", Input)
        if self._history_index is None:
            self._history_draft = input_widget.value
            self._history_index = len(self._command_history) - 1
        else:
            self._history_index = max(0, self._history_index - 1)

        previous = self._command_history[self._history_index]
        input_widget.value = previous
        input_widget.cursor_position = len(previous)

    def _show_next_command(self) -> None:
        if self._history_index is None:
            return

        input_widget = self.query_one("#command-input", Input)
        if self._history_index >= len(self._command_history) - 1:
            self._history_index = None
            input_widget.value = self._history_draft
            input_widget.cursor_position = len(input_widget.value)
            return

        self._history_index += 1
        next_command = self._command_history[self._history_index]
        input_widget.value = next_command
        input_widget.cursor_position = len(next_command)
