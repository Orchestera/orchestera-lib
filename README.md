# orchestera-lib

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

Exit the TUI with:

```text
/exit
```

Commands support fuzzy matching, so close variants like `/strt-proxy` also work.

`/update-cluster-context <cluster-name>` runs:

```bash
aws eks update-kubeconfig --name <cluster-name> --region us-east-1 --profile orchestera-dev
```

This starts:

```bash
kubectl port-forward svc/traefik -n traefik 8080:80 3080:3080 9001:8080
```

Forwarded local ports:

- `8080`
- `3080`
- `9001`
