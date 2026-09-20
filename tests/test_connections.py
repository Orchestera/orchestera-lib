import json
import logging
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestera.connections import (
    DEFAULT_CONNECTIONS_FILE,
    Connection,
    ConnectionsError,
    connections_file_path,
    get_connection,
    load_connections,
    spark_conf_for_connections,
    spark_conf_from_file,
)

BUCKET_KEY = "spark.hadoop.fs.s3a.bucket.analytics.access.key"

S3_ENTRY = {
    "name": "analytics",
    "kind": "S3",
    "region": "us-east-1",
    "network_mode": "PUBLIC",
    "host": "",
    "spark_conf": {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.bucket.analytics.endpoint.region": "us-east-1",
        BUCKET_KEY: "AKIAEXAMPLE",
        "spark.hadoop.fs.s3a.bucket.analytics.secret.key": "s3cret",
    },
}

DB_ENTRY = {
    "name": "warehouse",
    "kind": "POSTGRES",
    "region": "us-east-1",
    "network_mode": "PRIVATELINK",
    "host": "vpce-123.us-east-1.vpce.amazonaws.com",
    "jdbc": {
        "url": "jdbc:postgresql://vpce-123.us-east-1.vpce.amazonaws.com:5432/wh",
        "driver": "org.postgresql.Driver",
        "user": "orchestera",
        "password": "hunter2",
    },
}


class ConnectionsFileTestCase(unittest.TestCase):
    """Writes a connections file into a temporary directory for each test."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.directory = Path(self._tmp.name)

    def write(self, payload):
        path = self.directory / "connections.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return str(path)

    def absent(self):
        return str(self.directory / "absent.json")


class ConnectionsPathTests(ConnectionsFileTestCase):
    def test_explicit_path_wins_over_the_environment(self):
        with patch.dict(os.environ, {"ORCH_CONNECTIONS_FILE": "/from/env.json"}):
            self.assertEqual(connections_file_path("/explicit.json"), "/explicit.json")

    def test_environment_wins_over_the_default_mount(self):
        with patch.dict(os.environ, {"ORCH_CONNECTIONS_FILE": "/from/env.json"}):
            self.assertEqual(connections_file_path(), "/from/env.json")

    def test_falls_back_to_the_default_mount(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(connections_file_path(), DEFAULT_CONNECTIONS_FILE)


class LoadConnectionsTests(ConnectionsFileTestCase):
    def test_a_missing_file_means_no_connections_not_an_error(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(load_connections(self.absent()), [])

    def test_an_empty_array_loads_as_no_connections(self):
        self.assertEqual(load_connections(self.write([])), [])

    def test_loads_s3_and_database_entries(self):
        s3, db = load_connections(self.write([S3_ENTRY, DB_ENTRY]))

        self.assertEqual(s3.name, "analytics")
        self.assertEqual(s3.kind, "S3")
        self.assertEqual(s3.spark_conf, S3_ENTRY["spark_conf"])
        self.assertEqual(s3.jdbc, {})
        self.assertEqual(db.jdbc, DB_ENTRY["jdbc"])
        self.assertEqual(db.host, DB_ENTRY["host"])
        self.assertEqual(db.spark_conf, {})

    def test_reads_the_path_from_the_environment(self):
        path = self.write([S3_ENTRY])
        with patch.dict(os.environ, {"ORCH_CONNECTIONS_FILE": path}):
            self.assertEqual([c.name for c in load_connections()], ["analytics"])

    def test_entries_tolerate_missing_optional_keys(self):
        (connection,) = load_connections(self.write([{"name": "bare", "kind": "S3"}]))
        self.assertEqual(connection, Connection(name="bare", kind="S3"))

    def test_credentials_are_never_logged(self):
        with self.assertLogs("orchestera.connections", level="DEBUG") as logs:
            load_connections(self.write([S3_ENTRY, DB_ENTRY]))

        self.assertNotIn("AKIAEXAMPLE", "\n".join(logs.output))
        self.assertNotIn("hunter2", "\n".join(logs.output))


class MalformedConnectionsTests(ConnectionsFileTestCase):
    """A file that exists but cannot be used must fail loudly, not drop credentials."""

    def test_malformed_json_raises(self):
        path = self.directory / "connections.json"
        path.write_text("{not json", encoding="utf-8")
        with self.assertRaisesRegex(ConnectionsError, "not valid JSON"):
            load_connections(str(path))

    def test_a_non_array_document_raises(self):
        with self.assertRaisesRegex(ConnectionsError, "JSON array"):
            load_connections(self.write({"name": "analytics"}))

    def test_a_non_object_entry_raises(self):
        with self.assertRaisesRegex(ConnectionsError, "only JSON objects"):
            load_connections(self.write(["analytics"]))

    def test_an_entry_without_a_kind_raises(self):
        with self.assertRaisesRegex(ConnectionsError, "missing required key"):
            load_connections(self.write([{"name": "analytics"}]))


class SparkConfTests(ConnectionsFileTestCase):
    def test_merges_only_spark_conf_never_jdbc_credentials(self):
        conf = spark_conf_from_file(self.write([S3_ENTRY, DB_ENTRY]))

        self.assertEqual(conf, S3_ENTRY["spark_conf"])
        self.assertNotIn("hunter2", json.dumps(conf))

    def test_connections_for_different_buckets_compose(self):
        other = Connection(
            name="raw",
            kind="S3",
            spark_conf={"spark.hadoop.fs.s3a.bucket.raw.access.key": "AKIAOTHER"},
        )
        conf = spark_conf_for_connections([Connection.from_payload(S3_ENTRY), other])

        self.assertEqual(conf[BUCKET_KEY], "AKIAEXAMPLE")
        self.assertEqual(conf["spark.hadoop.fs.s3a.bucket.raw.access.key"], "AKIAOTHER")

    def test_two_connections_on_one_bucket_warn_and_last_wins(self):
        shared = "spark.hadoop.fs.s3a.bucket.shared.access.key"
        first = Connection(name="first", kind="S3", spark_conf={shared: "AKIAFIRST"})
        second = Connection(name="second", kind="S3", spark_conf={shared: "AKIASECOND"})

        with self.assertLogs("orchestera.connections", level=logging.WARNING) as logs:
            conf = spark_conf_for_connections([first, second])

        self.assertEqual(conf[shared], "AKIASECOND")
        self.assertIn("second", "\n".join(logs.output))
        self.assertNotIn("AKIASECOND", "\n".join(logs.output))

    def test_no_connections_contribute_no_conf(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(spark_conf_from_file(self.absent()), {})


class GetConnectionTests(ConnectionsFileTestCase):
    def test_returns_the_named_entry(self):
        path = self.write([S3_ENTRY, DB_ENTRY])
        self.assertEqual(get_connection("warehouse", path).jdbc["user"], "orchestera")

    def test_names_what_is_available(self):
        path = self.write([S3_ENTRY])
        with self.assertRaisesRegex(ConnectionsError, "Available: analytics"):
            get_connection("missing", path)

    def test_an_empty_mount_says_none(self):
        path = self.write([])
        with self.assertRaisesRegex(ConnectionsError, "Available: none"):
            get_connection("missing", path)


if __name__ == "__main__":
    unittest.main()
