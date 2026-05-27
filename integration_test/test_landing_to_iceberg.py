#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "confluent-kafka==2.3.0",
#     "psycopg2-binary==2.9.9",
#     "pyiceberg[s3fs]>=0.9.0",
#     "requests==2.31.0",
#     "pytest==8.2.0",
#     "pyarrow>=23.0.1",
#     "numpy>=2.4.4",
# ]
# ///
"""
Integration test: Kafka → Duckling → DuckLake → Exporter → Iceberg

Prerequisites: `docker compose up -d` is running from the project root.

Run:
    ./integration_test/test_landing_to_iceberg.py
    uv run integration_test/test_landing_to_iceberg.py
"""
import json
import os
import subprocess
import sys
import textwrap
import time
import uuid
import random

import psycopg2
import pyarrow as pa
import pytest
import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.io import load_file_io as _pyiceberg_load_file_io
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

# ---------------------------------------------------------------------------
# Patch: Lakekeeper returns Docker-internal hostnames (e.g., http://garage:3900/)
# in table config, which override the catalog-level s3.endpoint.  When running
# from the host, only localhost:3900 is reachable.  This patch makes an
# explicitly-configured catalog s3.endpoint survive the merge.
# ---------------------------------------------------------------------------
def _patched_catalog_load_file_io(self, properties=None, location=None):
    merged = {**self.properties, **(properties or {})}
    # s3.endpoint: keep catalog value so host-side tests reach localhost:3900,
    #              not the Docker-internal garage:3900 returned by Lakekeeper.
    if "s3.endpoint" in self.properties:
        merged["s3.endpoint"] = self.properties["s3.endpoint"]
    # Strip REST signer config so FsspecFileIO uses direct aiobotocore SigV4 signing,
    # which is compatible with Garage (unlike PyArrowFileIO's streaming SHA256 uploads).
    if "s3.access-key-id" in self.properties:
        for k in [k for k in merged if k.startswith("s3.signer")]:
            del merged[k]
    return _pyiceberg_load_file_io(merged, location)

RestCatalog._load_file_io = _patched_catalog_load_file_io

# ---------------------------------------------------------------------------
# Config — all overridable via environment variables
# ---------------------------------------------------------------------------
KAFKA_BROKERS  = os.environ.get("KAFKA_BROKERS",   "localhost:29092")
APICURIO_URL   = os.environ.get("APICURIO_URL",    "http://localhost:8080/apis/registry/v2")
POSTGRES_DSN   = os.environ.get("POSTGRES_DSN",    "postgresql://duckling:duckling@localhost:5432/ducklake")
ICEBERG_URL    = os.environ.get("ICEBERG_REST_URL", "http://localhost:8181")
S3_ENDPOINT    = os.environ.get("S3_ENDPOINT",     "http://localhost:3900")
AWS_KEY        = os.environ.get("AWS_ACCESS_KEY_ID",     "GK6475636b6c696e6700000000")
AWS_SECRET     = os.environ.get("AWS_SECRET_ACCESS_KEY", "6475636b6c696e6700000000000000000000000000000000000000000000dead")

# Unique suffix per test run — avoids any cleanup of previous run's tables.
RUN_SUFFIX = uuid.uuid4().hex[:8]

# The topic / table we test against
TOPIC     = f"test.orders_{RUN_SUFFIX}"
IS_GROUP  = "test"                   # Apicurio group id  (== IS name)
ARTIFACT  = TOPIC                    # Apicurio artifact id matches Kafka topic name

# Iceberg table as seen via the REST catalog (namespace.table)
# DuckLake fullTableName = {isName}__{tableName} where isName="test", tableName=f"orders_{RUN_SUFFIX}"
DUCKLAKE_TABLE = f"test__orders_{RUN_SUFFIX}"
ICEBERG_TABLE  = f"landing.{DUCKLAKE_TABLE}"

# How many records to produce (override with --count or RECORD_COUNT env var)
RECORD_COUNT = int(os.environ.get("RECORD_COUNT", "10000"))

# JSON Schema registered in Apicurio — defines field types for DuckLake column mapping
JSON_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "order_id": {"type": "string"},
        "amount":   {"type": "number"},
        "region":   {"type": "string"},
        "payload":  {"type": "string"},
    },
    "required": ["order_id", "amount", "region", "payload"],
    "additionalProperties": False,
}

REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compose_run(*cmd) -> subprocess.CompletedProcess:
    """Runs a one-off command via docker compose run --rm."""
    cwd = os.path.join(os.path.dirname(__file__), "..")
    args = ["docker", "compose", "run", "--rm"] + list(cmd)
    return subprocess.run(args, capture_output=True, text=True, cwd=cwd)


def _start_services() -> None:
    """Starts core services (kafka, postgres, garage, apicurio, duckling).
    Lakekeeper is started later by _run_lakekeeper_migrate() after the DB migration."""
    cwd = os.path.join(os.path.dirname(__file__), "..")
    result = subprocess.run(
        ["docker", "compose", "up", "-d", "--build",
         "kafka", "postgres", "garage", "apicurio", "duckling"],
        capture_output=True, text=True, cwd=cwd,
    )
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise RuntimeError(f"docker compose up failed (rc={result.returncode})")
    print("Core services started.")


def _run_lakekeeper_migrate() -> None:
    """Runs the Lakekeeper DB migration (idempotent) then starts lakekeeper."""
    cwd = os.path.join(os.path.dirname(__file__), "..")
    result = subprocess.run(
        ["docker", "compose", "run", "--rm", "lakekeeper-migrate"],
        capture_output=True, text=True, cwd=cwd,
    )
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise RuntimeError(f"lakekeeper-migrate failed (rc={result.returncode})")
    # Start lakekeeper and exporter now that the DB is migrated.
    subprocess.run(
        ["docker", "compose", "up", "-d", "lakekeeper", "exporter"],
        capture_output=True, text=True, cwd=cwd, check=True,
    )
    print("Lakekeeper migration complete.")


def _setup_garage() -> None:
    """Ensures Garage node is configured, bucket exists, and access key is imported."""
    cwd = os.path.join(os.path.dirname(__file__), "..")

    def _exec(*cmd):
        result = subprocess.run(
            ["docker", "compose", "exec", "garage"] + list(cmd),
            capture_output=True, text=True, cwd=cwd,
        )
        return result.stdout + result.stderr

    # Get node ID and assign layout (idempotent)
    status = _exec("/garage", "status")
    node_id = next((w for w in status.split() if len(w) == 16 and all(c in "0123456789abcdef" for c in w)), None)
    if node_id:
        _exec("/garage", "layout", "assign", "-z", "local", "-c", "1G", node_id)
        _exec("/garage", "layout", "apply", "--version", "1")

    # Import key with valid Garage format (GK prefix + 24 hex chars)
    _exec("/garage", "key", "import", "--yes",
          "GK6475636b6c696e6700000000",
          "6475636b6c696e6700000000000000000000000000000000000000000000dead",
          "-n", "duckling")

    # Create bucket and grant access (idempotent)
    _exec("/garage", "bucket", "create", "test")
    _exec("/garage", "bucket", "allow", "--read", "--write", "--owner", "test",
          "--key", "GK6475636b6c696e6700000000")
    print("Garage S3 configured.")


def _build_catalog() -> RestCatalog:
    """Returns a RestCatalog pointed at the local Lakekeeper instance."""
    return RestCatalog(
        name="landing",
        **{
            "uri": f"{ICEBERG_URL}/catalog",
            "warehouse": "landing",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_KEY,
            "s3.secret-access-key": AWS_SECRET,
            "s3.path-style-access": "true",
            "s3.region": "garage",
        },
    )


def _verify_iceberg_catalog() -> None:
    """Smoke-tests the catalog: write one row via PyArrow, read it back, then drop."""
    catalog = _build_catalog()
    table_id = ("landing", "_smoke_iceberg_test")

    try:
        catalog.drop_table(table_id)
    except Exception:
        pass

    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "msg", StringType(), required=False),
    )
    tbl = catalog.create_table(table_id, schema=schema)
    tbl.append(pa.table({"id": pa.array([1], pa.int64()), "msg": pa.array(["hello iceberg"])}))

    rows = tbl.scan().to_arrow().num_rows
    assert rows == 1, f"Iceberg smoke test: expected 1 row, got {rows}"

    catalog.drop_table(table_id)
    print("Iceberg catalog smoke test passed (write + read + drop OK).")


def _setup_lakekeeper() -> None:
    """Creates Lakekeeper project + warehouse if not already set up."""
    r = requests.get(f"{ICEBERG_URL}/catalog/v1/config?warehouse=landing")
    if r.status_code != 200:
        requests.post(f"{ICEBERG_URL}/management/v1/bootstrap",
                      json={"accept-terms-of-use": True})

        r = requests.post(f"{ICEBERG_URL}/management/v1/project",
                          json={"project-name": "default"})
        project_id = r.json().get("project-id", "00000000-0000-0000-0000-000000000000")

        requests.post(f"{ICEBERG_URL}/management/v1/warehouse", json={
            "warehouse-name": "landing",
            "project-id": project_id,
            "storage-profile": {
                "type": "s3",
                "bucket": "test",
                "endpoint": "http://garage:3900",
                "path-style-access": True,
                "region": "garage",
                "sts-enabled": False,
                "flavor": "minio",
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": AWS_KEY,
                "aws-secret-access-key": AWS_SECRET,
            },
        })

        pg = psycopg2.connect(POSTGRES_DSN)
        try:
            with pg.cursor() as cur:
                cur.execute("UPDATE warehouse SET project_id = '00000000-0000-0000-0000-000000000000'")
            pg.commit()
        finally:
            pg.close()

        print("Lakekeeper warehouse created.")
    else:
        print("Lakekeeper warehouse already configured.")

    catalog = _build_catalog()
    try:
        catalog.create_namespace("landing")
        print("Created Iceberg namespace 'landing'.")
    except NamespaceAlreadyExistsError:
        print("Iceberg namespace 'landing' already exists.")


def _write_duckling_config() -> None:
    """Writes local/duckling.yaml with the unique per-run topic/table names."""
    cwd = os.path.join(os.path.dirname(__file__), "..")
    config_path = os.path.join(cwd, "local", "duckling.yaml")
    content = textwrap.dedent(f"""\
        duckling:
          kafka:
            brokers: "kafka:9092"
            schema_registry: "http://apicurio:8080/apis/registry/v2"

          ducklake:
            catalog_url: "jdbc:postgresql://postgres:5432/ducklake?user=duckling&password=duckling"
            s3_endpoint: "http://garage:3900"
            s3_access_key: "GK6475636b6c696e6700000000"
            s3_secret_key: "6475636b6c696e6700000000000000000000000000000000000000000000dead"
            s3_region: "garage"

          metrics:
            prometheus:
              port: 9090

          adaptive:
            safety_factor: 0.65
            ema_alpha: 0.3

          topics:
            - name: {TOPIC}
              group_id: duckling-{TOPIC.replace(".", "-")}
              table: test.orders_{RUN_SUFFIX}
              min_flush_interval_seconds: 5
              max_flush_interval_seconds: 30
              metadata_poll_interval_seconds: 5
              offset_progress_flush_threshold: 0.5
              max_batch_bytes: 67108864
    """)
    with open(config_path, "w") as f:
        f.write(content)
    print(f"[setup] Wrote duckling.yaml for topic '{TOPIC}'.")


def _reset_test_state() -> None:
    """Points duckling at a fresh unique topic/table for this run and restarts it."""
    cwd = os.path.join(os.path.dirname(__file__), "..")

    _write_duckling_config()

    subprocess.run(["docker", "compose", "stop", "duckling"], cwd=cwd, check=True)
    print("Duckling stopped.")

    pg = psycopg2.connect(POSTGRES_DSN)
    try:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name LIKE 'ducklake_%'
            """)
            tables = [row[0] for row in cur.fetchall()]
        if tables:
            with pg.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {', '.join(tables)} CASCADE")
            pg.commit()
            print(f"[setup] Dropped {len(tables)} DuckLake catalog table(s).")
        else:
            print("[setup] DuckLake catalog already clean.")
    finally:
        pg.close()

    admin = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    futures = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1,
                                            config={"retention.bytes": str(10 * 1024 * 1024)})])
    for _, f in futures.items():
        try:
            f.result()
        except Exception as e:
            print(f"Note (topic create): {e}")
    print(f"Created topic '{TOPIC}'.")

    # Register schema before starting duckling so it's available on the first fetch attempt.
    _register_schema()
    _set_backward_compatibility(IS_GROUP, ARTIFACT)

    subprocess.run(["docker", "compose", "start", "duckling"], cwd=cwd, check=True)
    _wait_for_http("http://localhost:9090/metrics", timeout=60)
    print("Duckling started and ready.")


def _wait_for_http(url: str, timeout: int = 60, interval: int = 3) -> None:
    """Polls url until it returns 2xx or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code < 500:
                return
        except requests.RequestException:
            pass
        time.sleep(interval)
    raise TimeoutError(f"{url} not reachable after {timeout}s")


def _register_schema() -> None:
    """Registers the JSON Schema in Apicurio (idempotent)."""
    url = f"{APICURIO_URL}/groups/{IS_GROUP}/artifacts/{ARTIFACT}"
    if requests.get(url).status_code == 200:
        print(f"    Schema '{ARTIFACT}' already registered.")
        return

    resp = requests.post(
        f"{APICURIO_URL}/groups/{IS_GROUP}/artifacts",
        headers={
            "Content-Type": "application/json",
            "X-Registry-ArtifactId": ARTIFACT,
            "X-Registry-ArtifactType": "JSON",
        },
        json=JSON_SCHEMA,
    )
    resp.raise_for_status()
    print(f"    Schema '{ARTIFACT}' registered (global ID: {resp.json()['globalId']}).")


def _set_backward_compatibility(group_id: str, artifact_id: str) -> None:
    """Ensures the artifact has BACKWARD compatibility mode set."""
    url = f"{APICURIO_URL}/groups/{group_id}/artifacts/{artifact_id}/rules"
    resp = requests.post(url, json={"type": "COMPATIBILITY", "config": "BACKWARD"})
    if resp.status_code not in (200, 201, 204, 409):
        resp.raise_for_status()


def _produce_records(count: int) -> None:
    """Produces `count` plain JSON records to the Kafka topic."""
    producer = Producer({"bootstrap.servers": KAFKA_BROKERS})
    errors = []

    def on_delivery(err, _msg):
        if err:
            errors.append(err)

    for i in range(count):
        record = {
            "order_id": str(uuid.uuid4()),
            "amount":   round(random.uniform(1.0, 9999.0), 2),
            "region":   random.choice(REGIONS),
            "payload":  "x" * 900,   # padding — keeps each record ~1 KB
        }
        producer.produce(
            TOPIC,
            value=json.dumps(record).encode("utf-8"),
            on_delivery=on_delivery,
        )
        if i % 1000 == 0:
            producer.poll(0)

    producer.flush()
    if errors:
        raise RuntimeError(f"Producer errors: {errors}")
    print(f"Produced {count} records to '{TOPIC}'")


def _wait_for_ducklake_stable(stable_for: int = 8, timeout: int = 300,
                              target_rows: int | None = None) -> int:
    """Waits until data row count is stable. Returns the final snapshot_id."""
    pg = psycopg2.connect(POSTGRES_DSN)
    deadline = time.time() + timeout
    last_count = 0
    stable_since: float | None = None
    max_snap = 0
    try:
        while time.time() < deadline:
            try:
                with pg.cursor() as cur:
                    cur.execute("""
                        SELECT COALESCE(SUM(record_count), 0)
                        FROM ducklake_data_file
                        WHERE table_id = (
                            SELECT MAX(table_id) FROM ducklake_table WHERE table_name = %s
                        )
                        AND end_snapshot IS NULL
                    """, (DUCKLAKE_TABLE,))
                    row_count = cur.fetchone()[0]
                    cur.execute("SELECT COALESCE(MAX(snapshot_id), 0) FROM ducklake_snapshot")
                    max_snap = cur.fetchone()[0]
            except Exception:
                pg.rollback()
                time.sleep(2)
                continue

            now = time.time()
            elapsed = timeout - (deadline - now)

            if target_rows is not None and row_count >= target_rows:
                print(f"  [{elapsed:.1f}s] DuckLake: data rows={row_count}, snapshot={max_snap} ✓ target reached")
                return max_snap

            if row_count != last_count:
                last_count = row_count
                stable_since = now
                pct = f" ({100*row_count//target_rows}%)" if target_rows else ""
                print(f"  [{elapsed:.1f}s] DuckLake: data rows={row_count}{pct}, snapshot={max_snap} (changed)")
            else:
                if row_count > 0 and stable_since is not None:
                    stable_secs = now - stable_since
                    print(f"  [{elapsed:.1f}s] DuckLake: data rows={row_count}, snapshot={max_snap} (stable {stable_secs:.1f}s)")
                    if stable_secs >= stable_for:
                        return max_snap
                else:
                    print(f"  [{elapsed:.1f}s] DuckLake: data rows={row_count}, snapshot={max_snap} (waiting for data...)")
            time.sleep(2)
    finally:
        pg.close()
    return max_snap


EXPORTER_URL = os.environ.get("EXPORTER_URL", "http://localhost:9091")


def _prewarm_exporter() -> None:
    """Waits for the exporter HTTP server to be ready (initialization complete)."""
    _wait_for_http(f"{EXPORTER_URL}/health", timeout=120)
    print("Exporter server ready.")


def _run_exporter() -> None:
    """Triggers the exporter via HTTP POST /export."""
    r = requests.post(f"{EXPORTER_URL}/export", timeout=120)
    print(r.text[-3000:] if r.text else "")
    if r.status_code != 200:
        raise RuntimeError(f"Exporter failed (HTTP {r.status_code})")


def _count_iceberg_records(table_name: str) -> int:
    """Counts records in the Iceberg table via PyIceberg."""
    namespace, tbl = table_name.split(".", 1)
    return _build_catalog().load_table((namespace, tbl)).scan().to_arrow().num_rows


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

def test_data_lands_in_iceberg():
    """
    End-to-end: produce JSON records to Kafka → Duckling lands in DuckLake
    → Exporter exports to Iceberg → count matches.
    """
    timings: dict[str, float] = {}

    def step(label: str):
        """Returns (start_time, label) — call finish(start, label) when done."""
        print(f"\n[step] {label}...")
        return time.time(), label

    def finish(start: float, label: str):
        elapsed = time.time() - start
        timings[label] = elapsed
        print(f"[step] {label} done ({elapsed:.1f}s)")

    t, lbl = step("Start services (docker compose up + image pulls/builds)")
    _start_services()
    finish(t, lbl)

    t, lbl = step("Setup Garage S3")
    _setup_garage()
    finish(t, lbl)

    t, lbl = step("Wait for Apicurio")
    _wait_for_http(f"{APICURIO_URL}/groups", timeout=60)
    finish(t, lbl)

    t, lbl = step("Lakekeeper DB migration + start")
    _run_lakekeeper_migrate()
    finish(t, lbl)

    t, lbl = step("Wait for Lakekeeper catalog")
    _wait_for_http(f"{ICEBERG_URL}/catalog/v1/config", timeout=60)
    finish(t, lbl)

    t, lbl = step("Setup Lakekeeper warehouse + namespace")
    _setup_lakekeeper()
    finish(t, lbl)

    t, lbl = step("Iceberg catalog smoke test")
    _verify_iceberg_catalog()
    finish(t, lbl)

    t, lbl = step("Wait for exporter server ready")
    _prewarm_exporter()
    finish(t, lbl)

    t, lbl = step("Reset test state (stop duckling, drop catalog, create topic, start duckling)")
    _reset_test_state()
    finish(t, lbl)

    t, lbl = step("Register JSON schema in Apicurio (idempotent — already done in reset)")
    _register_schema()
    _set_backward_compatibility(IS_GROUP, ARTIFACT)
    finish(t, lbl)

    t, lbl = step(f"Produce {RECORD_COUNT} records to Kafka")
    _produce_records(RECORD_COUNT)
    finish(t, lbl)

    t, lbl = step("Duckling lands all records in DuckLake")
    final_snap = _wait_for_ducklake_stable(stable_for=8, timeout=300, target_rows=RECORD_COUNT)
    print(f"    DuckLake stable at snapshot {final_snap}.")
    finish(t, lbl)

    t, lbl = step("Run exporter (Iceberg registration)")
    _run_exporter()
    finish(t, lbl)

    t, lbl = step("Count records in Iceberg")
    iceberg_count = _count_iceberg_records(ICEBERG_TABLE)
    print(f"    Iceberg has {iceberg_count} record(s), expected {RECORD_COUNT}")
    finish(t, lbl)

    assert iceberg_count == RECORD_COUNT, (
        f"Expected {RECORD_COUNT} records in Iceberg but found {iceberg_count}"
    )

    total = sum(timings.values())
    print("\n" + "=" * 60)
    print("TIMING SUMMARY")
    print("=" * 60)
    for label, elapsed in timings.items():
        print(f"  {elapsed:6.1f}s  {label}")
    print("-" * 60)
    print(f"  {total:6.1f}s  TOTAL")
    print("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Duckling integration test")
    parser.add_argument("--count", type=int, default=10_000,
                        help="Number of records to produce (default: 10000)")
    args, remaining = parser.parse_known_args()
    os.environ["RECORD_COUNT"] = str(args.count)
    sys.exit(pytest.main([__file__, "-v", "-s"] + remaining))
