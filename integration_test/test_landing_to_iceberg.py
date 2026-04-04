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
import time
import uuid
import random

import duckdb
import fastavro
import psycopg2
import pytest
import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError

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

# The topic / table we test against
TOPIC     = "test.orders"
IS_GROUP  = "test"          # Apicurio group id  (== IS name)
ARTIFACT  = "test.orders"   # Apicurio artifact id

# Iceberg table as seen via the REST catalog (namespace.table)
ICEBERG_TABLE = "landing.test__orders"
DUCKLAKE_TABLE = "test__orders"

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


def _setup_lakekeeper() -> None:
    """Creates Lakekeeper project + warehouse if not already set up."""
    # If warehouse already resolves, nothing to do
    r = requests.get(f"{ICEBERG_URL}/catalog/v1/config?warehouse=landing")
    if r.status_code == 200:
        print("Lakekeeper already configured.")
        return

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

    print("Lakekeeper configured.")


def _drop_ducklake_table(table_name: str) -> None:
    """Drops a DuckLake table inline using duckdb+ducklake (no container spin-up)."""
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL ducklake; LOAD ducklake")
        conn.execute(f"""
            CREATE SECRET duckling_s3 (
                TYPE S3,
                KEY_ID '{AWS_KEY}',
                SECRET '{AWS_SECRET}',
                ENDPOINT 'localhost:3900',
                USE_SSL false,
                URL_STYLE 'path',
                REGION 'garage'
            )
        """)
        conn.execute(
            "ATTACH 'ducklake:postgres:host=localhost port=5432 dbname=ducklake"
            " user=duckling password=duckling' AS ducklake"
        )
        conn.execute(f'DROP TABLE IF EXISTS ducklake.main."{table_name}"')
        print(f"[cleanup] Dropped ducklake.main.{table_name}")
    except Exception as e:
        print(f"[cleanup] Note: {e}")
    finally:
        conn.close()


def _reset_test_state() -> None:
    """Cleans up state from previous test runs to ensure a clean slate."""
    cwd = os.path.join(os.path.dirname(__file__), "..")

    # 1. Stop duckling first — Kafka rejects consumer group deletion while members are active.
    subprocess.run(["docker", "compose", "stop", "duckling"], cwd=cwd, check=True)
    print("Duckling stopped.")

    # 2. Drop DuckLake table
    _drop_ducklake_table(DUCKLAKE_TABLE)

    # 3. Drop Iceberg table
    try:
        catalog = RestCatalog(name="landing", **{
            "uri": f"{ICEBERG_URL}/catalog",
            "warehouse": "landing",
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_KEY,
            "s3.secret-access-key": AWS_SECRET,
            "s3.path-style-access": "true",
        })
        catalog.drop_table(("landing", DUCKLAKE_TABLE))
        print(f"Dropped Iceberg table landing.{DUCKLAKE_TABLE}")
    except (NoSuchTableError, Exception) as e:
        print(f"Note (Iceberg drop): {e}")

    # 4. Delete consumer group (duckling is stopped, so group has no active members).
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    futures = admin.delete_consumer_groups(["duckling-test-orders"])
    for gid, f in futures.items():
        try:
            f.result()
            print(f"Deleted consumer group '{gid}'.")
        except Exception as e:
            print(f"Note (group delete '{gid}'): {e}")

    # 5. Delete and recreate Kafka topic so offsets start at 0.
    existing = admin.list_topics(timeout=10).topics
    if TOPIC in existing:
        futures = admin.delete_topics([TOPIC])
        for _, f in futures.items():
            try:
                f.result()
            except Exception:
                pass
        time.sleep(2)
    futures = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1,
                                            config={"retention.bytes": str(1 * 1024 * 1024)})])
    for _, f in futures.items():
        try:
            f.result()
        except Exception:
            pass
    print(f"Recreated topic '{TOPIC}'.")

    # 6. Start duckling — fresh consumer, no committed offsets, earliest resets to 0.
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


def _create_topic_if_missing(topic: str, retention_bytes: int = 1 * 1024 * 1024) -> None:
    """Creates Kafka topic if it doesn't exist yet."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        return

    new_topic = NewTopic(
        topic,
        num_partitions=1,
        replication_factor=1,
        config={"retention.bytes": str(retention_bytes)},
    )
    futures = admin.create_topics([new_topic])
    for t, f in futures.items():
        f.result()
    print(f"Created topic '{topic}' with retention.bytes={retention_bytes}")


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


def _get_max_snapshot_id() -> int:
    """Returns the current maximum snapshot_id from DuckLake catalog."""
    pg = psycopg2.connect(POSTGRES_DSN)
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(snapshot_id), 0) FROM ducklake_snapshot")
            return cur.fetchone()[0]
    finally:
        pg.close()


def _duckling_records_consumed() -> float:
    """Reads duckling_records_consumed_total from Prometheus metrics endpoint."""
    try:
        r = requests.get("http://localhost:9090/metrics", timeout=2)
        for line in r.text.splitlines():
            if line.startswith("duckling_records_consumed_total"):
                return float(line.split()[-1])
    except Exception:
        pass
    return -1.0


def _wait_for_ducklake_landing(ducklake_table: str, baseline_snapshot: int = 0, timeout: int = 80) -> int:
    """Polls Postgres until a new DuckLake snapshot appears after `baseline_snapshot`.
    This means duckling did at least one flush (data write) after we started waiting.
    Returns the latest snapshot_id when done.
    """
    pg = psycopg2.connect(POSTGRES_DSN)
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            max_snap = 0
            try:
                with pg.cursor() as cur:
                    cur.execute("SELECT COALESCE(MAX(snapshot_id), 0) FROM ducklake_snapshot")
                    max_snap = cur.fetchone()[0]
            except Exception:
                pg.rollback()

            consumed = _duckling_records_consumed()
            elapsed = timeout - (deadline - time.time())
            print(f"  [{elapsed:.1f}s] DuckLake: max_snapshot={max_snap} (baseline={baseline_snapshot}) | consumed={consumed:.0f}")
            if max_snap > baseline_snapshot:
                print(f"  → New snapshot detected: {max_snap}")
                return max_snap
            time.sleep(2)
    finally:
        pg.close()
    raise TimeoutError(
        f"DuckLake: no new snapshot after baseline={baseline_snapshot} within {timeout}s. "
        "Is Duckling running with the test topic configured?"
    )


def _wait_for_ducklake_stable(baseline_snapshot: int, stable_for: int = 8, timeout: int = 30) -> int:
    """Waits until the max snapshot_id stops increasing for `stable_for` seconds.
    Returns the final max snapshot_id.
    """
    pg = psycopg2.connect(POSTGRES_DSN)
    deadline = time.time() + timeout
    last_snap = baseline_snapshot
    stable_since = time.time()
    try:
        while time.time() < deadline:
            try:
                with pg.cursor() as cur:
                    cur.execute("SELECT COALESCE(MAX(snapshot_id), 0) FROM ducklake_snapshot")
                    max_snap = cur.fetchone()[0]
            except Exception:
                pg.rollback()
                time.sleep(2)
                continue

            now = time.time()
            elapsed = timeout - (deadline - now)
            if max_snap != last_snap:
                last_snap = max_snap
                stable_since = now
                print(f"  [{elapsed:.1f}s] DuckLake: new snapshot → {max_snap}")
            else:
                stable_secs = now - stable_since
                print(f"  [{elapsed:.1f}s] DuckLake: snapshot={max_snap} (stable {stable_secs:.1f}s)")
                if stable_secs >= stable_for:
                    return max_snap
            time.sleep(2)
    finally:
        pg.close()
    return last_snap


def _run_exporter() -> None:
    """Runs the exporter as a one-off container via docker compose run --rm."""
    result = _compose_run("exporter", "run", "exporter.py")
    print(result.stdout[-3000:] if result.stdout else "")
    if result.returncode != 0:
        print(result.stderr[-2000:])
        raise RuntimeError(f"Exporter failed (rc={result.returncode})")


def _count_iceberg_records(table_name: str) -> int:
    """Counts records in the Iceberg table via PyIceberg."""
    catalog = RestCatalog(
        name="landing",
        **{
            "uri": f"{ICEBERG_URL}/catalog",
            "warehouse": "landing",
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_KEY,
            "s3.secret-access-key": AWS_SECRET,
            "s3.path-style-access": "true",
        },
    )
    namespace, tbl = table_name.split(".", 1)
    table = catalog.load_table((namespace, tbl))
    return table.scan().to_arrow().num_rows


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

    # 2. Reset state from previous runs
    print("[2] Resetting test state...")
    _reset_test_state()

    # 3. Register schema and set BACKWARD compatibility
    print("[3] Registering Avro schema in Apicurio...")
    global_id = _register_schema(None)
    _set_backward_compatibility(IS_GROUP, ARTIFACT)
    print(f"    Schema global ID: {global_id}")

    # 4. Topic was recreated by _reset_test_state; ensure it exists (no-op if already there).
    print("[4] Ensuring Kafka topic exists...")
    _create_topic_if_missing(TOPIC, retention_bytes=1 * 1024 * 1024)

    # 5. Capture baseline snapshot, then produce records.
    #    auto.offset.reset=earliest: duckling will read from offset 0 regardless of when it subscribes.
    baseline = _get_max_snapshot_id()
    print(f"[5] Producing {RECORD_COUNT} records (~10 MB)... (baseline snapshot: {baseline})")
    _produce_records(global_id, RECORD_COUNT)

    # 6. Wait for Duckling to flush at least one batch to DuckLake
    print("[6] Waiting for Duckling to land data in DuckLake (first flush)...")
    _wait_for_ducklake_landing(DUCKLAKE_TABLE, baseline_snapshot=baseline, timeout=80)

    # 7. Wait for snapshot count to stabilise (no more in-flight batches)
    print("[7] Waiting for all batches to flush (snapshot count stable)...")
    final_snaps = _wait_for_ducklake_stable(baseline_snapshot=baseline, stable_for=8, timeout=30)
    print(f"    DuckLake stable at {final_snaps} snapshot(s).")

    # 8. Run the exporter
    print("[8] Running exporter...")
    _run_exporter()

    # 9. Validate record count in Iceberg
    print("[9] Counting records in Iceberg...")
    iceberg_count = _count_iceberg_records(ICEBERG_TABLE)
    print(f"    Iceberg has {iceberg_count} record(s), expected {RECORD_COUNT}")

    assert iceberg_count == RECORD_COUNT, (
        f"Expected {RECORD_COUNT} records in Iceberg but found {iceberg_count}"
    )
