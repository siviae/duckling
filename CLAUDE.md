# Duckling — CLAUDE.md

## What This Is

**Duckling** is a Kafka-to-DuckLake landing pipeline. It reads Avro messages from Kafka topics, accumulates them into micro-batches, and writes Parquet files to DuckLake (Postgres catalog + S3 storage). A separate exporter writes DuckLake data into Iceberg via a REST catalog (Lakekeeper).

```
Kafka (Avro) → [Duckling: validate → batch → flush] → DuckLake (Parquet/S3)
                                                            ↓ (Exporter)
                                                        Iceberg (Lakekeeper REST catalog)
```

## Architecture

- **One coroutine per Kafka topic** under a `SupervisorJob` — failures are isolated per topic
- **Three flush triggers** (whichever fires first): time elapsed, offset progress (lag %), bytes accumulated
- **Adaptive memory controller** (`AdaptiveController.kt`): dynamically computes per-topic batch size budget from JVM heap, number of active topics, and EMA throughput — prevents OOM
- **Schema governance via Apicurio**: BACKWARD-compatible Avro schemas, fetched and applied before each flush; schema changes trigger ALTER TABLE DDL

## Module Layout

```
duckling-landing/src/main/kotlin/io/company/duckling/
├── Main.kt                  # Entry point: config loading, Prometheus server, coroutine supervisor
├── Pipeline.kt              # Per-topic state machine: poll → validate → accumulate → flush → commit
├── AdaptiveController.kt    # EMA throughput estimator, per-topic heap budget
├── config/
│   ├── Config.kt            # YAML data classes
│   └── ConfigLoader.kt      # Jackson YAML parser
├── source/
│   ├── KafkaSource.kt       # KafkaConsumer wrapper: poll, watermarks, commitSync
│   └── SchemaValidator.kt   # Apicurio fetch, Avro→DuckDB type mapping, validation rules
└── sink/
    ├── DuckLakeWriter.kt    # DuckDB JDBC: attach DuckLake catalog, prepared-statement inserts
    └── SchemaEvolution.kt   # ALTER TABLE DDL: add columns, widen BIGINT→DOUBLE; blocks non-widening changes
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Kotlin 2.2.0, JVM 21 |
| Build | Gradle (Kotlin DSL), configuration cache enabled |
| Async | Kotlin Coroutines 1.8.0 |
| Kafka | kafka-clients 3.7.0 |
| Schema Registry | Apicurio 3.0.15 (BACKWARD compat) |
| Serialization | Avro 1.11.3 |
| Data engine | DuckDB JDBC 1.5.1.0 (in-process, in-memory) |
| Storage | Garage v1.0.1 (S3-compatible), region = `garage` |
| Catalog | DuckLake 1.5.x extension (Postgres catalog, S3 Parquet storage) |
| Iceberg catalog | Lakekeeper (`quay.io/lakekeeper/catalog:latest-main`) |
| Metrics | Micrometer Prometheus 1.12.0 |
| Config | Jackson YAML 2.17.0 |

## Local Dev Setup

```bash
docker compose up -d
# Then run integration test:
cd integration_test
uv run pytest test_landing_to_iceberg.py -s
```

Build duckling JAR:
```bash
./gradlew :duckling-landing:jar
```

After code changes, rebuild and restart:
```bash
./gradlew :duckling-landing:jar && docker compose up -d --build duckling
```

## Garage S3 Credentials

Garage requires key IDs in format: `GK` + 24 hex chars (12 bytes). The key in use:
- **Key ID**: `GK6475636b6c696e6700000000`  (="duckling" in hex + zero padding)
- **Secret**: `6475636b6c696e6700000000000000000000000000000000000000000000dead`
- **Bucket**: `test`
- **Region**: `garage` (configured in `local/garage.toml` as `s3_region = "garage"`)
- **Endpoint** (internal/Docker): `http://garage:3900`
- **Endpoint** (host): `http://localhost:3900`

**IMPORTANT**: The old key `duckling-local-key-id-00000001` is INVALID — Garage rejects non-`GK` prefixed key IDs. The `garage key import` in `garage-init` was silently failing because of this.

## Lakekeeper Setup

Lakekeeper requires a project and warehouse to be created before tables can be stored.

Current setup (done once, persisted in Postgres `ducklake` DB):
- **Project**: `00000000-0000-0000-0000-000000000000` ("Default Project") — Lakekeeper uses this as the default project for unauthenticated requests
- **Warehouse**: `landing` (id: `dd6337e0-2e21-11f1-946c-5b83eb0575ba`) in default project, pointing to S3 bucket `test`

The project and warehouse creation must be automated in the test setup. See `_setup_lakekeeper()` in the integration test.

REST catalog URL for PyIceberg: `http://localhost:8181` (host) or `http://lakekeeper:8181` (Docker). Use `warehouse="landing"`.

## DuckLake Notes (v1.5.x)

### ATTACH syntax (changed in 1.5.x):
```sql
-- OLD (pre-1.5): ATTACH '...' AS ducklake (TYPE DUCKLAKE)
-- NEW (1.5.x):
ATTACH 'ducklake:postgres:host=postgres port=5432 dbname=ducklake user=duckling password=duckling' AS ducklake
  (DATA_PATH 's3://test/ducklake/orders/')
```

### S3 secret must be created BEFORE ATTACH:
```sql
CREATE SECRET duckling_s3 (
    TYPE S3,
    KEY_ID 'GK6475636b6c696e6700000000',
    SECRET '6475636b6c696e6700000000000000000000000000000000000000000000dead',
    ENDPOINT 'garage:3900',
    USE_SSL false,
    URL_STYLE 'path',
    REGION 'garage'
)
```

### Inline data vs Parquet:
DuckLake uses inline storage (data in Postgres `ducklake_inlined_data_*` tables) when S3 is not accessible or for small batches. Data only goes to S3 Parquet when S3 credentials work. If DuckLake writes inline, `ducklake_data_file` stays empty (count = 0) but `ducklake_inlined_data_1_1` has rows.

Check what data is stored:
```sql
-- Inline rows:
SELECT COUNT(*) FROM ducklake_inlined_data_1_1;
-- S3 file rows:
SELECT COUNT(*) FROM ducklake_data_file;
```

### Extension install (core, NOT community):
```sql
INSTALL ducklake;   -- NOT: INSTALL ducklake FROM community
LOAD ducklake;
```

## Known Issues / Remaining Work

### 1. KafkaConsumer Thread-Safety Bug (CRITICAL — pipeline crashes)
**File**: `duckling-landing/src/main/kotlin/io/company/duckling/Pipeline.kt`

**Problem**: `source.highWatermarks()`, `source.currentOffsets()`, and `source.commitSync()` are called inside `withContext(Dispatchers.IO)`, which schedules them on different threads than the main consumer coroutine that calls `source.poll()`. KafkaConsumer is not thread-safe.

**Error**: `ERROR Pipeline -- Stopping topic 'test.orders': flush_failed: KafkaConsumer is not safe for multi-threaded access`

**Fix**: Use a single-threaded Kotlin coroutine context (e.g., `newSingleThreadContext("kafka-${cfg.name}")`) for ALL Kafka operations (poll, highWatermarks, currentOffsets, commitSync). Use `Dispatchers.IO` only for `writer.writeBatch()`.

**Current code fragment** (lines 121-175 in Pipeline.kt):
```kotlin
val watermarks = withContext(Dispatchers.IO) { source.highWatermarks() }  // WRONG
val committedOffs = withContext(Dispatchers.IO) { source.currentOffsets() }  // WRONG
withContext(Dispatchers.IO) {
    writer.writeBatch(batch, currentSchema.columns)
    source.commitSync()  // WRONG — Kafka op on wrong thread
}
```

**Fix pattern**:
```kotlin
// Create at top of runTopicPipeline:
val kafkaContext = newSingleThreadContext("kafka-${cfg.name}")

// Use for ALL kafka ops:
val watermarks = withContext(kafkaContext) { source.highWatermarks() }
// producer coroutine:
val producer = launch(kafkaContext) { while (isActive) { source.poll(...) } }
// flush:
withContext(kafkaContext) { source.commitSync() }
// writer stays on IO dispatcher
withContext(Dispatchers.IO) { writer.writeBatch(batch, currentSchema.columns) }
```

### 2. Exporter Broken (uses wrong table names, missing Postgres table)
**File**: `duckling-exporter/exporter.py`

**Problems**:
- Uses `ducklake_data_files` (should be `ducklake_data_file`, singular)
- Uses `ducklake_snapshots` (should be `ducklake_snapshot`, singular)
- References `ducklake_export_state` table (doesn't exist — custom table that needs to be created)
- `ducklake_data_file` has no `table_name` column (uses `table_id` + join to `ducklake_table`)
- `ducklake_data_file.path` not `file_path`
- ATTACH uses old `TYPE DUCKLAKE` syntax (broken in 1.5.x)
- Uses Spark for reading Parquet (overkill; should use DuckDB directly)
- Inline data (stored in Postgres, not S3 Parquet) is invisible to the Spark file reader

**Fix**: Rewrite exporter to use DuckDB with DuckLake extension to read tables (handles both inline and Parquet transparently), then write to Iceberg via PyIceberg. Remove Spark dependency.

**New exporter pattern**:
```python
import duckdb
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError

conn = duckdb.connect()
conn.execute("INSTALL ducklake; LOAD ducklake")
conn.execute("CREATE SECRET ...")  # S3 credentials
conn.execute("ATTACH 'ducklake:postgres:...' AS ducklake")

catalog = RestCatalog("landing", uri=ICEBERG_REST_URL, warehouse="landing", ...)

tables = conn.execute("SHOW TABLES FROM ducklake.main").fetchall()
for (table_name,) in tables:
    df = conn.execute(f"SELECT * FROM ducklake.main.{table_name}").fetch_arrow_table()
    ns = "landing"
    try:
        tbl = catalog.load_table((ns, table_name))
        tbl.overwrite(df)
    except NoSuchTableError:
        tbl = catalog.create_table((ns, table_name), schema=df.schema)
        tbl.append(df)
    print(f"Exported {len(df)} rows to {ns}.{table_name}")
```

Also update `pyproject.toml` to use `duckdb>=1.5.1` (not `>=1.1.3`).

### 3. Test Cleanup / Idempotency
The integration test needs a `_reset_test_state()` function to run before producing records, to ensure a clean slate:
1. Delete and recreate the Kafka topic (clears old offsets and records)
2. Restart duckling (so it re-subscribes with `auto.offset.reset=earliest`)
3. Truncate DuckLake table: `DELETE FROM ducklake.main.test__orders` via DuckDB
4. Drop Iceberg table: `catalog.drop_table(("landing", "test__orders"))` via PyIceberg

Without cleanup, previous test runs leave stale data and the record count check fails.

### 4. garage-init fix (docker-compose.yml)
The `garage key import` in `garage-init` fails because `duckling-local-key-id-00000001` is not a valid Garage key ID format. Fix the `garage-init` service to use:
```
/garage key import --yes GK6475636b6c696e6700000000 6475636b6c696e6700000000000000000000000000000000000000000000dead -n duckling
```

### 5. _wait_for_ducklake_landing must check inline data too
Current test checks only `ducklake_data_file` (S3 Parquet files). DuckLake also stores data in `ducklake_inlined_data_1_1`. Check both:
```python
cur.execute("""
    SELECT COUNT(*) FROM ducklake_inlined_data_1_1 i
    JOIN ducklake_inlined_data_tables t ON t.table_name = 'ducklake_inlined_data_1_1'
    JOIN ducklake_table dt ON dt.table_id = t.table_id
    WHERE dt.table_name = %s
""", (ducklake_name,))
```
Or simpler: check `ducklake_snapshot` count has increased.

### 6. Lakekeeper bootstrap in test setup
Must be automated. The test needs a `_setup_lakekeeper()` function:
```python
def _setup_lakekeeper():
    # Check if already set up
    r = requests.get(f"{ICEBERG_URL}/catalog/v1/config?warehouse=landing")
    if r.status_code == 200:
        return

    # Bootstrap if needed
    requests.post(f"{ICEBERG_URL}/management/v1/bootstrap",
                  json={"accept-terms-of-use": True})

    # Create project
    r = requests.post(f"{ICEBERG_URL}/management/v1/project",
                      json={"project-name": "default"})
    project_id = r.json().get("project-id", "00000000-0000-0000-0000-000000000000")

    # Create warehouse
    requests.post(f"{ICEBERG_URL}/management/v1/warehouse", json={
        "warehouse-name": "landing",
        "project-id": project_id,
        "storage-profile": {
            "type": "s3", "bucket": "test", "endpoint": f"http://garage:3900",
            "path-style-access": True, "region": "garage", "sts-enabled": False, "flavor": "minio"
        },
        "storage-credential": {
            "type": "s3", "credential-type": "access-key",
            "aws-access-key-id": AWS_KEY, "aws-secret-access-key": AWS_SECRET,
        },
    })

    # Move warehouse to default project so catalog API resolves it
    # (Lakekeeper uses project 00000000-0000-0000-0000-000000000000 for unauthenticated requests)
    pg = psycopg2.connect(POSTGRES_DSN)
    with pg.cursor() as cur:
        cur.execute("UPDATE warehouse SET project_id = '00000000-0000-0000-0000-000000000000'")
    pg.commit()
    pg.close()
```

## Configuration

Config file passed via `--config <path>` (default: `duckling.yaml`).

Key per-topic settings (`local/duckling.yaml`):
- `min_flush_interval_seconds` / `max_flush_interval_seconds` — time-based flush bounds
- `offset_progress_flush_threshold` — flush when this fraction of lag is consumed (e.g. `0.5`)
- `max_batch_bytes` — upper bound; adaptive controller may lower it based on heap

## Error Handling Conventions

- Schema validation failure → stop topic coroutine, emit `duckling_topic_stopped_total`
- Schema evolution with non-widening type change → throw, stop topic
- Write failure → stop topic (no silent data loss)
- Offset commit happens **only after** successful write; no at-most-once risk

## Key Metrics (Prometheus on port 9090)

- `duckling_records_consumed_total` — records read from Kafka (by topic)
- `duckling_flushes_total` — successful DuckLake writes (by topic)
- `duckling_batch_flushed_bytes` — histogram of batch sizes (by topic)
- `duckling_topic_stopped_total` — topic pipeline stops (schema errors, write failures)
- `duckling_adaptive_max_batch_bytes` — current adaptive limit (by topic)
- `duckling_adaptive_max_interval_seconds` — current adaptive flush interval (by topic)

## Integration Test

Located at `integration_test/test_landing_to_iceberg.py`. Run with:
```bash
cd integration_test && uv run pytest test_landing_to_iceberg.py -s
```

Test flow:
1. Setup Garage S3 (key + bucket)
2. Setup Lakekeeper (project + warehouse)
3. Wait for Apicurio
4. Register Avro schema
5. Create Kafka topic
6. Produce 10K records (~10MB)
7. Wait for Duckling to flush to DuckLake
8. Wait extra 35s for remaining batches
9. Run exporter
10. Count Iceberg records → assert == 10K

## Garage Admin Commands

```bash
# Inside the garage container:
docker compose exec garage /garage key list
docker compose exec garage /garage key info --show-secret GK6475636b6c696e6700000000
docker compose exec garage /garage bucket list
docker compose exec garage /garage bucket allow --read --write --owner test --key GK6475636b6c696e6700000000

# From host (with valid credentials):
AWS_ACCESS_KEY_ID=GK6475636b6c696e6700000000 \
AWS_SECRET_ACCESS_KEY=6475636b6c696e6700000000000000000000000000000000000000000000dead \
aws --endpoint-url http://localhost:3900 s3 ls s3://test/ --recursive
```

## DuckLake Inspection

```sql
-- Via psql:
docker compose exec postgres psql -U duckling -d ducklake

SELECT table_name FROM ducklake_table;
SELECT COUNT(*) FROM ducklake_inlined_data_1_1;  -- inline data
SELECT COUNT(*) FROM ducklake_data_file;           -- S3 parquet files
SELECT * FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 5;
SELECT changes_made FROM ducklake_snapshot_changes ORDER BY snapshot_id DESC LIMIT 10;
```
