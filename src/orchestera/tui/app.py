import subprocess
import threading
from difflib import get_close_matches

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.events import Key
from textual.widgets import Input, Static


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
    }

    #banner {
        padding: 0 1;
    }

    #status {
        height: 1;
        padding: 0 1;
    }

    #links {
        padding: 0 1;
        height: 4;
    }

    #command-input {
        height: 3;
    }

    #suggestion {
        padding: 0 1;
        height: 1;
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
        "/exit": "_exit_app",
    }

    def __init__(self) -> None:
        super().__init__()
        self._proxy_process: subprocess.Popen[str] | None = None
        self._proxy_links_announced = False

    def compose(self) -> ComposeResult:
        yield Static(self.ASCII_ART, id="banner")
        yield Static("Proxy status: stopped", id="status")
        yield Static(self._proxy_links_text(), id="links")
        yield Input(
            placeholder="Type a command and press Enter (Tab to autocomplete)",
            id="command-input",
        )
        yield Static(self._commands_hint(), id="suggestion")

    def on_mount(self) -> None:
        self.title = "Orchestera TUI"
        self.notify("Ready. Run /start-proxy to start proxy port-forward.", timeout=4)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        command = event.value.strip()
        event.input.value = ""
        self._set_suggestion(self._commands_hint())
        if not command:
            return

        command_name, args = self._parse_command(command)
        if not command_name:
            return

        resolved_command = self._resolve_command(command_name)
        if resolved_command is None:
            self.notify(
                "Unknown command. Use /start-proxy, /stop-proxy, /update-cluster-context, or /exit."
            )
            return

        if resolved_command != command_name:
            self.notify(f"Using closest match: {resolved_command}", timeout=2)

        handler_name = self.COMMANDS[resolved_command]
        getattr(self, handler_name)(args)

    def on_input_changed(self, event: Input.Changed) -> None:
        text = event.value.strip()
        if not text:
            self._set_suggestion(self._commands_hint())
            return

        command_name, _ = self._parse_command(text)
        if not command_name:
            self._set_suggestion(self._commands_hint())
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

    def _set_status(self, status: str) -> None:
        self.query_one("#status", Static).update(f"Proxy status: {status}")

    def _set_suggestion(self, message: str) -> None:
        self.query_one("#suggestion", Static).update(message)

    def _commands_hint(self) -> str:
        return "Commands: /start-proxy /stop-proxy /update-cluster-context <cluster-name> /exit"

    def _proxy_links_text(self) -> str:
        return (
            "JupyterHub: http://localhost:8080/jupyterhub\n"
            "Spark History Server: http://localhost:8080/spark-history\n"
            "AI Debugger: http://localhost:3080"
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
