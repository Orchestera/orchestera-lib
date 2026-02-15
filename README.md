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
