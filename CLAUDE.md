# Duckling — CLAUDE.md

## What This Is

**Duckling** is a Kafka-to-DuckLake landing pipeline. It reads plain JSON messages from Kafka topics, accumulates them into micro-batches, and writes Parquet files to DuckLake (Postgres catalog + S3 storage). A separate exporter registers those Parquet files with an Iceberg REST catalog (Lakekeeper) via pure metadata manipulation — no data is re-read.

```
Kafka (JSON) → [Duckling: validate → batch → flush] → DuckLake (Parquet/S3)
                                                            ↓ (Exporter)
                                                        Iceberg (Lakekeeper REST catalog)
```

## Message Format

Kafka messages are plain UTF-8 JSON objects — no magic byte prefix, no schema ID header.

```json
{"order_id": "abc-123", "amount": 42.5, "region": "eu-west-1"}
```

`KafkaSource` uses `StringDeserializer` and parses each message with Jackson `ObjectMapper` into `Map<String, Any?>`.

Schema is registered in Apicurio as a **JSON Schema** (draft-07) artifact (type `JSON`). It is used to:
- derive DuckLake column types: `string`→VARCHAR, `number`→DOUBLE, `integer`→BIGINT, `boolean`→BOOLEAN
- enforce BACKWARD compatibility on schema evolution

Fields absent from `"required"` are nullable. The union type `["T", "null"]` is also nullable.

## Architecture

- **One coroutine per Kafka topic** under a `SupervisorJob` — failures are isolated per topic
- **Three flush triggers** (whichever fires first): time elapsed, offset progress (lag %), bytes accumulated
- **Adaptive memory controller** (`AdaptiveController.kt`): dynamically computes per-topic batch size budget from JVM heap, number of active topics, and EMA throughput — prevents OOM
- **Schema governance via Apicurio**: BACKWARD-compatible JSON Schemas, fetched and applied before each flush; schema changes trigger ALTER TABLE DDL
- **Exporter — metadata only**: reads file paths/counts from `ducklake_data_file` in Postgres, calls PyIceberg `fast_append` to register files in Lakekeeper. Never reads Parquet data.

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
│   ├── KafkaSource.kt       # KafkaConsumer wrapper: poll (parses JSON→Map), watermarks, commitSync
│   └── SchemaValidator.kt   # Apicurio fetch, JSON Schema→DuckDB type mapping, validation rules
└── sink/
    ├── DuckLakeWriter.kt    # DuckDB JDBC: attach DuckLake catalog, staged batch inserts
    └── SchemaEvolution.kt   # ALTER TABLE DDL: add columns, widen BIGINT→DOUBLE; blocks non-widening changes

duckling-exporter/
├── exporter.py              # Metadata-only export: Postgres → Iceberg fast_append
└── schema_sync.py           # Utility: sync Iceberg schema from Apicurio (not called by exporter)

integration_test/
└── test_landing_to_iceberg.py  # End-to-end pytest: 1M records Kafka → DuckLake → Iceberg
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Kotlin 2.2.0, JVM 21 |
| Build | Gradle (Kotlin DSL), fat JAR via `tasks.jar` |
| Async | Kotlin Coroutines 1.8.0 |
| Kafka | kafka-clients 3.7.0 |
| Schema Registry | Apicurio 2.5.0.Final (JSON Schema, BACKWARD compat, v2 API) |
| Serialization | JSON (UTF-8), parsed by Jackson ObjectMapper |
| Data engine | DuckDB JDBC 1.5.1.0 (in-process, in-memory) |
| Storage (local) | Garage v1.0.1 (S3-compatible), region = `garage` |
| Catalog | DuckLake 1.5.x extension (Postgres catalog, S3 Parquet storage) |
| Iceberg catalog | Lakekeeper (`quay.io/lakekeeper/catalog:latest-main`) |
| Metrics | Micrometer Prometheus 1.12.0 |
| Config | Jackson YAML 2.17.0 |
| Exporter | Python 3.12, uv, duckdb>=1.5.1, pyiceberg[pyarrow]>=0.9.0, psycopg2-binary |

## Local Dev Setup

```bash
docker compose up -d
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

## Kafka Thread-Safety Model

`KafkaConsumer` is not thread-safe. All Kafka operations run on a dedicated single-threaded executor:

```kotlin
// Pipeline.kt — at coroutine scope start
val kafkaExecutor = Executors.newSingleThreadExecutor { r -> Thread(r, "kafka-${cfg.name}") }
val kafkaContext = kafkaExecutor.asCoroutineDispatcher()

// Producer loop runs on kafkaContext
val producer = launch(kafkaContext) { while (isActive) { source.poll(...) } }

// Watermark/offset checks also use kafkaContext
val watermarks = withContext(kafkaContext) { source.highWatermarks() }
withContext(kafkaContext) { source.commitSync() }

// Only DuckLake writes use Dispatchers.IO
withContext(Dispatchers.IO) { writer.writeBatch(batch, currentSchema.columns) }
```

The producer coroutine calls `yield()` after each `poll()` so that `withContext(kafkaContext)` calls from the consumer loop can be scheduled on the same thread.

## KafkaRecord Type

`KafkaSource.poll()` returns `List<KafkaRecord>` where:

```kotlin
data class KafkaRecord(
    val partition: Int,
    val offset: Long,
    val value: Map<String, Any?>,
)
```

`Pipeline.kt` uses `record.partition`, `record.offset`, `record.value` (no method calls, data class fields). The `batch` is `MutableList<Map<String, Any?>>`.

## JSON Schema in Apicurio

### Registration (artifact type `JSON`)

```http
POST /apis/registry/v2/groups/{group}/artifacts
Content-Type: application/json
X-Registry-ArtifactId: {topic-name}
X-Registry-ArtifactType: JSON

{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "order_id": {"type": "string"},
    "amount":   {"type": "number"},
    "count":    {"type": "integer"},
    "active":   {"type": "boolean"},
    "note":     {"type": ["string", "null"]}
  },
  "required": ["order_id", "amount"]
}
```

### Type mapping (SchemaValidator.kt)

| JSON Schema type | DuckDB type | Nullable |
|-----------------|-------------|---------|
| `"string"` | VARCHAR | if not in `required` |
| `"number"` | DOUBLE | if not in `required` |
| `"integer"` | BIGINT | if not in `required` |
| `"boolean"` | BOOLEAN | if not in `required` |
| `["T", "null"]` | (mapped T) | always nullable |

## DuckLake Write Strategy

Each flush stages records into a temp in-memory DuckDB table, then inserts in one shot:

```sql
CREATE OR REPLACE TEMP TABLE _batch_stage (col1 TYPE1, col2 TYPE2, ...);
-- JDBC prepared statement batch insert into _batch_stage
INSERT INTO ducklake.main.{fullTableName} SELECT * FROM _batch_stage;
DROP TABLE IF EXISTS _batch_stage;
```

This single `INSERT...SELECT` lets DuckLake manage its own transaction and creates exactly one Parquet snapshot per flush. Direct multi-row JDBC inserts into DuckLake do not create snapshots.

`DATA_INLINING_ROW_LIMIT 0` is set on ATTACH — all data goes to S3 Parquet (never inline in Postgres).

## DuckLake Notes (v1.5.x)

### ATTACH syntax:
```sql
-- S3 secret MUST be created before ATTACH
CREATE SECRET duckling_s3 (
    TYPE S3,
    KEY_ID '<access-key>',
    SECRET '<secret-key>',
    ENDPOINT '<s3-host:port>',
    USE_SSL false,
    URL_STYLE 'path',
    REGION '<region>'
)

ATTACH 'ducklake:postgres:host=postgres port=5432 dbname=ducklake user=duckling password=duckling' AS ducklake
  (DATA_PATH 's3://{isName}/ducklake/', DATA_INLINING_ROW_LIMIT 0, OVERRIDE_DATA_PATH TRUE)
```

### Extension install (core, NOT community):
```sql
INSTALL ducklake;   -- NOT: INSTALL ducklake FROM community
LOAD ducklake;
```

### Table naming:
`TopicConfig.table = "test.orders"` → `isName="test"`, `tableName="orders"` → DuckLake table `test__orders`, S3 path `s3://test/ducklake/main/test__orders/`.

### DuckLake schema tables (correct names for v1.5.x):
- `ducklake_table` — table registry
- `ducklake_data_file` — S3 Parquet file entries (not `ducklake_data_files`)
- `ducklake_snapshot` — snapshot history (not `ducklake_snapshots`)

## Exporter Design

`exporter.py` uses pure metadata manipulation — no Parquet data is read:

1. Reads `ducklake_data_file` from Postgres directly (paths, record counts, file sizes)
2. Calls `DESCRIBE ducklake.main."{table_name}"` via DuckDB to get the schema
3. Loads or creates the Iceberg table in Lakekeeper
4. Deduplicates against the current Iceberg snapshot (skips already-registered files)
5. Calls `fast_append` to register new files — writes only a small manifest to S3

The `schema_sync.py` utility exists but is **not called by `exporter.py`**. The exporter builds the Iceberg schema from `DESCRIBE` output on first table creation, but does not evolve schema for existing tables.

### S3 endpoint patch in PyIceberg:
Lakekeeper returns Docker-internal hostnames in table config that would override `s3.endpoint`. Both `exporter.py` and the integration test patch `RestCatalog._load_file_io` to strip REST signer config (incompatible with Garage) and preserve explicitly-set `s3.endpoint`.

## Garage S3 (local dev only)

Garage is used as a local S3-compatible object store for development and testing. It is not part of the production stack.

Garage requires key IDs in format: `GK` + 24 hex chars (12 bytes).
- **Key ID**: `GK6475636b6c696e6700000000`  (="duckling" in hex + zero padding)
- **Secret**: `6475636b6c696e6700000000000000000000000000000000000000000000dead`
- **Bucket**: `test`
- **Region**: `garage` (configured in `local/garage.toml` as `s3_region = "garage"`)
- **Endpoint** (internal/Docker): `http://garage:3900`
- **Endpoint** (host): `http://localhost:3900`

**IMPORTANT**: Garage rejects key IDs that don't start with `GK`. The `garage-init` service in `docker-compose.yml` correctly uses the `GK`-prefixed key.

### Garage Admin Commands

```bash
docker compose exec garage /garage key list
docker compose exec garage /garage key info --show-secret GK6475636b6c696e6700000000
docker compose exec garage /garage bucket list
docker compose exec garage /garage bucket allow --read --write --owner test --key GK6475636b6c696e6700000000

# From host:
AWS_ACCESS_KEY_ID=GK6475636b6c696e6700000000 \
AWS_SECRET_ACCESS_KEY=6475636b6c696e6700000000000000000000000000000000000000000000dead \
aws --endpoint-url http://localhost:3900 s3 ls s3://test/ --recursive
```

## Lakekeeper Setup

Lakekeeper requires a project and warehouse before tables can be stored. The integration test `_setup_lakekeeper()` handles this idempotently:
1. Bootstrap via `POST /management/v1/bootstrap`
2. Create project → get `project_id`
3. Create warehouse `landing` pointing to S3 bucket `test` (Garage in local dev)
4. SQL patch: `UPDATE warehouse SET project_id = '00000000-0000-0000-0000-000000000000'` — Lakekeeper uses this UUID as the default project for unauthenticated requests

REST catalog URL: `http://localhost:8181/catalog` (host) or `http://lakekeeper:8181/catalog` (Docker). Always set `warehouse="landing"`.

## Integration Test

Located at `integration_test/test_landing_to_iceberg.py`. Run with:
```bash
cd integration_test && uv run pytest test_landing_to_iceberg.py -s
```

Test flow:
1. Setup Garage S3 (key + bucket) via `docker compose exec garage`
2. Setup Lakekeeper (project + warehouse) — idempotent
3. Smoke-test Iceberg catalog (write 1 row, read back, drop)
4. Reset state: write unique `duckling.yaml`, stop duckling, drop all `ducklake_*` Postgres tables, create fresh Kafka topic, restart duckling
5. Register JSON Schema in Apicurio (`X-Registry-ArtifactType: JSON`)
6. Produce 1,000,000 plain JSON records (`json.dumps(record).encode("utf-8")`)
7. Wait for Duckling to flush all records to DuckLake (`_wait_for_ducklake_stable` polls `ducklake_data_file.record_count`)
8. Run exporter via `docker compose run --rm exporter`
9. Count Iceberg records via PyIceberg → assert == 1,000,000

Each test run uses a unique `RUN_SUFFIX` (UUID hex) so topic and table names never collide with previous runs.

## Configuration

Config file passed via `--config <path>` (default: `duckling.yaml`).

Key per-topic settings:
- `min_flush_interval_seconds` / `max_flush_interval_seconds` — time-based flush bounds
- `metadata_poll_interval_seconds` — how often to check Kafka offset progress
- `offset_progress_flush_threshold` — flush when this fraction of lag is consumed (e.g. `0.5`)
- `max_batch_bytes` — upper bound; adaptive controller may lower it based on heap
- `table` — `{isName}.{tableName}` format; DuckLake table = `{isName}__{tableName}`

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

## Known Issues / Remaining Work

### 1. `schema_sync.py` is not wired into the exporter
`duckling-exporter/schema_sync.py` implements Avro→Iceberg schema evolution but needs updating for JSON Schema, and `exporter.py` never calls it. When an existing Iceberg table has schema drift vs. DuckLake, the exporter logs a warning and proceeds without applying DDL.

**Fix**: update `schema_sync.py` for JSON Schema format; call `sync_iceberg_schema()` in `exporter.py` after loading an existing table, before `fast_append`.

### 2. `KafkaSource.highWatermarks()` and `currentOffsets()` both call `partitionsFor()` separately
Both methods call `consumer.partitionsFor(topic)` independently — two broker round-trips when one would suffice.

**Fix** (minor optimization, not a correctness issue): combine into a single method that returns both maps.

### 3. `DuckLakeWriter.ensureTable()` is called twice at pipeline startup
`Pipeline.kt` calls `writer.ensureTable(initialSchema.columns)` at init time, and then again on every flush. The first call is harmless (CREATE TABLE IF NOT EXISTS) but redundant.

### 4. Exporter data path reconstruction assumes `{isName}__` prefix
`exporter.py` splits `table_name` on `__` to derive `isName` for the S3 `data_path`. If a DuckLake table name does not contain `__`, relative file paths won't be resolved.

### 5. `_wait_for_ducklake_stable` only counts S3 Parquet files
The test waits on `ducklake_data_file.record_count`. This is correct for the current setup because `DATA_INLINING_ROW_LIMIT 0` is configured. If inlining is ever re-enabled, the test will wait forever.

## DuckLake Inspection

```bash
docker compose exec postgres psql -U duckling -d ducklake
```

```sql
SELECT table_name FROM ducklake_table;
SELECT COUNT(*) FROM ducklake_data_file;           -- S3 Parquet file entries
SELECT * FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 5;
SELECT changes_made FROM ducklake_snapshot_changes ORDER BY snapshot_id DESC LIMIT 10;
```
