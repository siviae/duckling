"""
Integration test: Kafka → Duckling → DuckLake → Exporter → Iceberg

Prerequisites: `docker compose up -d` is running from the project root.

Run:
    cd integration_test
    uv run pytest test_landing_to_iceberg.py -s
"""
import io
import os
import struct
import subprocess
import textwrap
import time
import uuid
import random

import fastavro
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

# How many records to produce (≈1 KB each → ~10 MB total)
RECORD_COUNT = 10_000

# Avro schema registered in Apicurio
AVRO_SCHEMA = {
    "type": "record",
    "name": "Order",
    "namespace": "test",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "amount",   "type": "double"},
        {"name": "region",   "type": "string"},
        {"name": "payload",  "type": "string"},
    ],
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
    # Bootstrap warehouse (idempotent)
    r = requests.get(f"{ICEBERG_URL}/catalog/v1/config?warehouse=landing")
    if r.status_code != 200:
        # Bootstrap (idempotent — ignore errors if already done)
        requests.post(f"{ICEBERG_URL}/management/v1/bootstrap",
                      json={"accept-terms-of-use": True})

        # Create project
        r = requests.post(f"{ICEBERG_URL}/management/v1/project",
                          json={"project-name": "default"})
        project_id = r.json().get("project-id", "00000000-0000-0000-0000-000000000000")

        # Create warehouse pointing to Garage S3 bucket
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

        # Move warehouse to the default project so unauthenticated catalog requests resolve it
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

    # Ensure 'landing' namespace exists (always — namespace may be missing even if warehouse exists)
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
    """Points duckling at a fresh unique topic/table for this run and restarts it.

    Since every run uses a new topic and table name (via RUN_SUFFIX), there is
    nothing to clean up from previous runs — old tables are simply left in place.
    """
    cwd = os.path.join(os.path.dirname(__file__), "..")

    # 1. Write duckling.yaml with the unique topic/table for this run.
    _write_duckling_config()

    # 2. Stop duckling so we can restart with the new config.
    subprocess.run(["docker", "compose", "stop", "duckling"], cwd=cwd, check=True)
    print("Duckling stopped.")

    # 3. Drop all DuckLake catalog tables so DuckLake re-initialises on the next ATTACH.
    #    TRUNCATE leaves empty tables that confuse DuckLake; DROP lets it start fresh.
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

    # 4. Create the new Kafka topic (fresh, no old offsets).
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    futures = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1,
                                            config={"retention.bytes": str(10 * 1024 * 1024)})])
    for _, f in futures.items():
        try:
            f.result()
        except Exception as e:
            print(f"Note (topic create): {e}")
    print(f"Created topic '{TOPIC}'.")

    # 5. Start duckling — fresh consumer pointing at the new topic.
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


def _register_schema(global_id_if_exists: int | None) -> int:
    """Registers the Avro schema in Apicurio and returns the global ID."""
    url = f"{APICURIO_URL}/groups/{IS_GROUP}/artifacts/{ARTIFACT}"
    resp = requests.get(url)
    if resp.status_code == 200:
        meta = requests.get(f"{url}/meta").json()
        return meta["globalId"]

    resp = requests.post(
        f"{APICURIO_URL}/groups/{IS_GROUP}/artifacts",
        headers={
            "Content-Type": "application/vnd.apache.avro+json",
            "X-Registry-ArtifactId": ARTIFACT,
            "X-Registry-ArtifactType": "AVRO",
        },
        json=AVRO_SCHEMA,
    )
    resp.raise_for_status()
    return resp.json()["globalId"]


def _set_backward_compatibility(group_id: str, artifact_id: str) -> None:
    """Ensures the artifact has BACKWARD compatibility mode set."""
    url = f"{APICURIO_URL}/groups/{group_id}/artifacts/{artifact_id}/rules"
    resp = requests.post(url, json={"type": "COMPATIBILITY", "config": "BACKWARD"})
    if resp.status_code not in (200, 201, 204, 409):
        resp.raise_for_status()



def _serialize(schema_dict: dict, record: dict, global_id: int) -> bytes:
    """
    Apicurio v2 wire format:
      0x00  (1 byte  magic)
      id    (8 bytes big-endian long — global artifact ID)
      avro  (schemaless Avro binary)
    """
    parsed = fastavro.parse_schema(schema_dict)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, record)
    avro_bytes = buf.getvalue()
    return struct.pack(">bq", 0x00, global_id) + avro_bytes


def _produce_records(global_id: int, count: int) -> None:
    """Produces `count` Avro records to the Kafka topic."""
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
            value=_serialize(AVRO_SCHEMA, record, global_id),
            on_delivery=on_delivery,
        )
        if i % 1000 == 0:
            producer.poll(0)

    producer.flush()
    if errors:
        raise RuntimeError(f"Producer errors: {errors}")
    print(f"Produced {count} records to '{TOPIC}'")


def _wait_for_ducklake_stable(stable_for: int = 8, timeout: int = 120) -> int:
    """Waits until the DuckLake data file count for DUCKLAKE_TABLE has been non-zero
    and stable for `stable_for` seconds.  Returns the final snapshot_id.

    Checks ducklake_data_file directly so we don't mistake the CREATE TABLE snapshot
    (which has no data files yet) for a completed data flush.
    """
    pg = psycopg2.connect(POSTGRES_DSN)
    deadline = time.time() + timeout
    last_count = 0
    stable_since: float | None = None
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
            if row_count != last_count:
                last_count = row_count
                stable_since = now
                print(f"  [{elapsed:.1f}s] DuckLake: data rows={row_count}, snapshot={max_snap} (changed)")
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
    return max_snap  # type: ignore[return-value]


def _run_exporter() -> None:
    """Runs the exporter as a one-off container via docker compose run --rm."""
    result = _compose_run("exporter", "run", "exporter.py")
    print(result.stdout[-3000:] if result.stdout else "")
    if result.returncode != 0:
        print(result.stderr[-2000:])
        raise RuntimeError(f"Exporter failed (rc={result.returncode})")


def _count_iceberg_records(table_name: str) -> int:
    """Counts records in the Iceberg table via PyIceberg."""
    namespace, tbl = table_name.split(".", 1)
    return _build_catalog().load_table((namespace, tbl)).scan().to_arrow().num_rows


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

def test_data_lands_in_iceberg():
    """
    End-to-end: produce ~10 MB to Kafka → Duckling lands in DuckLake
    → Exporter exports to Iceberg → count matches.
    """
    # 0. Ensure Garage S3 is initialized
    print("\n[0] Setting up Garage S3...")
    _setup_garage()

    # 1. Wait for dependent services to be up
    print("[1] Waiting for services...")
    _wait_for_http(f"{APICURIO_URL}/groups", timeout=60)
    _wait_for_http(f"{ICEBERG_URL}/catalog/v1/config", timeout=60)

    # 1b. Set up Lakekeeper (idempotent)
    print("[1b] Setting up Lakekeeper...")
    _setup_lakekeeper()

    # 1c. Verify Iceberg catalog works end-to-end (write + read + drop)
    print("[1c] Verifying Iceberg catalog (smoke test)...")
    _verify_iceberg_catalog()

    # 2. Reset state from previous runs
    print("[2] Resetting test state...")
    _reset_test_state()

    # 3. Register schema and set BACKWARD compatibility
    print("[3] Registering Avro schema in Apicurio...")
    global_id = _register_schema(None)
    _set_backward_compatibility(IS_GROUP, ARTIFACT)
    print(f"    Schema global ID: {global_id}")

    # 4. Produce records — topic was created by _reset_test_state.
    print(f"[4] Producing {RECORD_COUNT} records (~10 MB) to '{TOPIC}'...")
    _produce_records(global_id, RECORD_COUNT)

    # 5. Wait for Duckling to flush actual data files to DuckLake (not just DDL snapshot).
    print("[5] Waiting for Duckling to land data in DuckLake...")
    final_snap = _wait_for_ducklake_stable(stable_for=8, timeout=120)
    print(f"    DuckLake stable with data at snapshot {final_snap}.")

    # 6. Run the exporter
    print("[6] Running exporter...")
    _run_exporter()

    # 7. Validate record count in Iceberg
    print("[7] Counting records in Iceberg...")
    iceberg_count = _count_iceberg_records(ICEBERG_TABLE)
    print(f"    Iceberg has {iceberg_count} record(s), expected {RECORD_COUNT}")

    assert iceberg_count == RECORD_COUNT, (
        f"Expected {RECORD_COUNT} records in Iceberg but found {iceberg_count}"
    )
