"""
Integration test: Kafka → Duckling → DuckLake → Spark Exporter → Iceberg

Prerequisites: `docker compose up -d` is running from the project root.

Run:
    cd integration_test
    uv run pytest test_landing_to_iceberg.py
"""
import io
import os
import struct
import subprocess
import time
import uuid
import random

import fastavro
import psycopg2
import pytest
import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ConfigEntry
from pyiceberg.catalog.rest import RestCatalog

# ---------------------------------------------------------------------------
# Config — all overridable via environment variables
# ---------------------------------------------------------------------------
KAFKA_BROKERS  = os.environ.get("KAFKA_BROKERS",   "localhost:9092")
APICURIO_URL   = os.environ.get("APICURIO_URL",    "http://localhost:8080/apis/registry/v2")
POSTGRES_DSN   = os.environ.get("POSTGRES_DSN",    "postgresql://duckling:duckling@localhost:5432/ducklake")
ICEBERG_URL    = os.environ.get("ICEBERG_REST_URL", "http://localhost:8181")
S3_ENDPOINT    = os.environ.get("S3_ENDPOINT",     "http://localhost:3900")
AWS_KEY        = os.environ.get("AWS_ACCESS_KEY_ID",     "duckling-local-key-id-00000001")
AWS_SECRET     = os.environ.get("AWS_SECRET_ACCESS_KEY", "duckling-local-secret-0000000000000000000000000000000000000001")

# The topic / table we test against
TOPIC     = "test.orders"
IS_GROUP  = "test"          # Apicurio group id  (== IS name)
ARTIFACT  = "test.orders"   # Apicurio artifact id

# Iceberg table as seen via the REST catalog (namespace.table)
ICEBERG_TABLE = "landing.test__orders"

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

def _setup_garage() -> None:
    """Ensures Garage node is configured, bucket exists, and access key is imported."""
    cwd = os.path.join(os.path.dirname(__file__), "..")

    def _exec(*cmd):
        result = subprocess.run(
            ["docker", "compose", "exec", "garage"] + list(cmd),
            capture_output=True, text=True, cwd=cwd,
        )
        return result.stdout + result.stderr

    # Get node ID and assign layout (idempotent — fails silently if already done)
    status = _exec("/garage", "status")
    node_id = next((w for w in status.split() if len(w) == 16 and all(c in "0123456789abcdef" for c in w)), None)
    if node_id:
        _exec("/garage", "layout", "assign", "-z", "local", "-c", "1G", node_id)
        _exec("/garage", "layout", "apply", "--version", "1")

    # Import key with known credentials (idempotent)
    _exec("/garage", "key", "import", "--yes",
          "duckling-local-key-id-00000001",
          "duckling-local-secret-0000000000000000000000000000000000000001",
          "-n", "duckling")

    # Create bucket and grant access (idempotent)
    _exec("/garage", "bucket", "create", "test")
    _exec("/garage", "bucket", "allow", "--read", "--write", "--owner", "test",
          "--key", "duckling-local-key-id-00000001")
    print("Garage S3 configured.")


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
    # Check if already registered
    url = f"{APICURIO_URL}/groups/{IS_GROUP}/artifacts/{ARTIFACT}"
    resp = requests.get(url)
    if resp.status_code == 200:
        # Fetch meta to get globalId
        meta = requests.get(f"{url}/meta").json()
        return meta["globalId"]

    # Register new
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
    # 409 = rule already exists, that's fine
    if resp.status_code not in (200, 201, 204, 409):
        resp.raise_for_status()


def _create_topic_if_missing(topic: str, retention_bytes: int = 1 * 1024 * 1024) -> None:
    """Creates Kafka topic with small retention if it doesn't exist yet."""
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
        f.result()  # raises on error
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


def _wait_for_ducklake_landing(table_name: str, min_files: int = 1, timeout: int = 90) -> None:
    """Polls Postgres until at least `min_files` DuckLake data files are registered.

    DuckLake 1.5.x stores table names in ducklake_table as IS__topic__parts format.
    The input table_name is IS_GROUP.ARTIFACT e.g. "test.test.orders".
    We convert: drop IS_GROUP prefix -> "test.orders", replace "." with "__" -> "test__orders".
    """
    # Convert "is_group.topic.name" -> "topic__name" (DuckLake table_name format)
    _, artifact = table_name.split(".", 1)
    ducklake_name = artifact.replace(".", "__")

    pg = psycopg2.connect(POSTGRES_DSN)
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            with pg.cursor() as cur:
                cur.execute(
                    """SELECT COUNT(*) FROM ducklake_data_file f
                       JOIN ducklake_table t ON f.table_id = t.table_id
                       WHERE t.table_name = %s""",
                    (ducklake_name,),
                )
                count = cur.fetchone()[0]
            if count >= min_files:
                print(f"DuckLake has {count} file(s) for '{table_name}'")
                return
            time.sleep(3)
    finally:
        pg.close()
    raise TimeoutError(
        f"DuckLake still has no files for '{table_name}' after {timeout}s. "
        "Is Duckling running with the test topic configured?"
    )


def _run_exporter() -> None:
    """Triggers the exporter inside the 'spark' Docker Compose service via uv."""
    result = subprocess.run(
        [
            "docker", "compose", "exec",
            "-e", "DUCKLING_MIN_EXPORT_BYTES=0",
            "-e", "DUCKLING_MAX_EXPORT_AGE_MIN=0",
            "spark",
            "uv", "run", "exporter.py",
        ],
        capture_output=True,
        text=True,
        cwd=os.path.join(os.path.dirname(__file__), ".."),
    )
    print(result.stdout[-3000:] if result.stdout else "")
    if result.returncode != 0:
        print(result.stderr[-2000:])
        raise RuntimeError(f"Spark exporter failed (rc={result.returncode})")


def _count_iceberg_records(table_name: str) -> int:
    """Counts records in the Iceberg table via PyIceberg."""
    catalog = RestCatalog(
        name="landing",
        **{
            "uri": ICEBERG_URL,
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
    → Spark exporter exports to Iceberg → count matches.
    """
    # 0. Ensure Garage S3 is initialized
    print("\n[0] Setting up Garage S3...")
    _setup_garage()

    # 1. Wait for dependent services to be up
    print("[1] Waiting for Apicurio...")
    _wait_for_http(f"{APICURIO_URL}/groups", timeout=60)

    # 2. Register schema and set BACKWARD compatibility
    print("[2] Registering Avro schema in Apicurio...")
    global_id = _register_schema(None)
    _set_backward_compatibility(IS_GROUP, ARTIFACT)
    print(f"    Schema global ID: {global_id}")

    # 3. Create Kafka topic with 1 MB retention
    print("[3] Creating Kafka topic...")
    _create_topic_if_missing(TOPIC, retention_bytes=1 * 1024 * 1024)

    # 4. Produce ~10 MB of records
    print(f"[4] Producing {RECORD_COUNT} records (~10 MB)...")
    _produce_records(global_id, RECORD_COUNT)

    # 5. Wait for Duckling to flush at least one batch to DuckLake
    #    (the min flush interval is 30 s; wait up to 90 s)
    print("[5] Waiting for Duckling to land data in DuckLake...")
    _wait_for_ducklake_landing(f"{IS_GROUP}.{ARTIFACT}", min_files=1, timeout=90)

    # 6. Give Duckling extra time to flush remaining batches (max_flush=30s default)
    print("[6] Waiting 35 s for any remaining batches to flush...")
    time.sleep(35)

    # 7. Run the Spark exporter with zero thresholds so it exports immediately
    print("[7] Running Spark exporter...")
    _run_exporter()

    # 8. Validate record count in Iceberg
    print("[8] Counting records in Iceberg...")
    iceberg_count = _count_iceberg_records(ICEBERG_TABLE)
    print(f"    Iceberg has {iceberg_count} record(s), expected {RECORD_COUNT}")

    assert iceberg_count == RECORD_COUNT, (
        f"Expected {RECORD_COUNT} records in Iceberg but found {iceberg_count}"
    )
