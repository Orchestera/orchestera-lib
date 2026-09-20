"""
Read the external connections the control plane mounts into a workload pod.

Orchestera writes one ``connections.json`` per namespace into the
``orchestera-connections`` Secret and mounts it read-only at
``ORCH_CONNECTIONS_FILE``. The file holds only the connections belonging to
the workspace that namespace is bound to, so anything read here is already
scoped to the caller's workspace.

The file is a JSON array; each entry carries the config its consumer wants:

.. code-block:: json

    [
      {
        "name": "analytics",
        "kind": "S3",
        "region": "us-east-1",
        "network_mode": "PUBLIC",
        "host": "",
        "spark_conf": {"spark.hadoop.fs.s3a.bucket.analytics.access.key": "..."}
      }
    ]

S3 entries carry ``spark_conf`` (per-bucket ``s3a`` keys), database entries
carry ``jdbc`` (options for ``spark.read.format("jdbc")``).

Every value in ``spark_conf`` and ``jdbc`` is a live credential. Nothing in
this module logs them, and callers should not either.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_CONNECTIONS_FILE = "/var/run/orchestera/connections/connections.json"

S3 = "S3"


class ConnectionsError(Exception):
    """The mounted connections file exists but could not be read."""


@dataclass(frozen=True)
class Connection:
    """One external connection the workspace has configured."""

    name: str
    kind: str
    region: str = ""
    network_mode: str = ""
    host: str = ""
    spark_conf: Dict[str, str] = field(default_factory=dict)
    jdbc: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "Connection":
        try:
            name = payload["name"]
            kind = payload["kind"]
        except KeyError as exc:
            raise ConnectionsError(
                f"Connection entry is missing required key {exc}."
            ) from exc
        return cls(
            name=name,
            kind=kind,
            region=payload.get("region", ""),
            network_mode=payload.get("network_mode", ""),
            host=payload.get("host", ""),
            spark_conf=dict(payload.get("spark_conf") or {}),
            jdbc=dict(payload.get("jdbc") or {}),
        )


def connections_file_path(path: Optional[str] = None) -> str:
    """The path to read: explicit argument, else ``ORCH_CONNECTIONS_FILE``, else the default mount."""
    return path or os.environ.get("ORCH_CONNECTIONS_FILE") or DEFAULT_CONNECTIONS_FILE


def load_connections(path: Optional[str] = None) -> List[Connection]:
    """
    Every connection mounted into this pod, or ``[]`` when none are configured.

    A missing file means the workspace has no connections -- that is normal
    and not an error. A file that exists but cannot be parsed raises
    ``ConnectionsError``: credentials were meant to be here, and silently
    dropping them turns into a puzzling permission denied much later.
    """
    resolved = connections_file_path(path)
    try:
        with open(resolved, encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        logger.debug("No connections file at %s", resolved)
        return []
    except OSError as exc:
        raise ConnectionsError(f"Could not read {resolved}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ConnectionsError(f"{resolved} is not valid JSON: {exc}") from exc

    if not isinstance(payload, list):
        raise ConnectionsError(f"{resolved} must contain a JSON array of connections.")

    connections = [
        Connection.from_payload(entry) for entry in _as_dicts(payload, resolved)
    ]
    if connections:
        logger.info(
            "Loaded %d external connection(s): %s",
            len(connections),
            ", ".join(f"{c.name} ({c.kind})" for c in connections),
        )
    return connections


def _as_dicts(payload: List[Any], resolved: str) -> List[Dict[str, Any]]:
    for entry in payload:
        if not isinstance(entry, dict):
            raise ConnectionsError(f"{resolved} must contain only JSON objects.")
    return payload


def spark_conf_for_connections(connections: List[Connection]) -> Dict[str, str]:
    """
    Merge every connection's ``spark_conf`` into one dict for the Spark builder.

    The keys are per-bucket (``spark.hadoop.fs.s3a.bucket.<bucket>.*``), so
    connections for different buckets compose, and each bucket's own
    credentials provider takes precedence over the global one without
    disturbing it -- buckets reached through Pod Identity keep working.
    """
    merged: Dict[str, str] = {}
    for connection in connections:
        overlap = merged.keys() & connection.spark_conf.keys()
        if overlap:
            logger.warning(
                "Connection %r overrides Spark conf already set by an earlier "
                "connection (%d key(s)); check for two connections on one bucket.",
                connection.name,
                len(overlap),
            )
        merged.update(connection.spark_conf)
    return merged


def spark_conf_from_file(path: Optional[str] = None) -> Dict[str, str]:
    """``spark_conf_for_connections(load_connections(path))`` -- what the session applies."""
    return spark_conf_for_connections(load_connections(path))


def get_connection(name: str, path: Optional[str] = None) -> Connection:
    """
    One connection by name, for reading a database from a notebook::

        conn = get_connection("warehouse")
        df = spark.read.format("jdbc").options(**conn.jdbc).option("dbtable", "t").load()

    S3 needs no lookup: its conf is already on the session.
    """
    connections = load_connections(path)
    for connection in connections:
        if connection.name == name:
            return connection
    known = ", ".join(sorted(c.name for c in connections)) or "none"
    raise ConnectionsError(f"No connection named {name!r}. Available: {known}.")
