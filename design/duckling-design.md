# Duckling — Design Document

> **Duckling** is a data landing pipeline that moves data from team services into the company data platform. It ingests micro-batches from Kafka, buffers them in DuckLake for fresh querying, and exports compacted data to Iceberg for analytical consumption via Trino and Spark.

---

## Table of Contents

1. [Context & Goals](#1-context--goals)
2. [Architecture Overview](#2-architecture-overview)
3. [Component Design](#3-component-design)
   - 3.1 [Kotlin sql-flow Pipeline](#31-kotlin-sql-flow-pipeline)
   - 3.2 [DuckLake Store](#32-ducklake-store)
   - 3.3 [Spark Exporter](#33-spark-exporter)
   - 3.4 [Iceberg](#34-iceberg)
   - 3.5 [Query Layer](#35-query-layer)
4. [Schema Evolution](#4-schema-evolution)
5. [Dead Letter Queue](#5-dead-letter-queue)
6. [Configuration DSL](#6-configuration-dsl)
7. [Performance Considerations](#7-performance-considerations)
8. [Project Structure](#8-project-structure)
9. [Technology Stack](#9-technology-stack)
10. [Open Questions](#10-open-questions)

---

## 1. Context & Goals

### Background

The company data platform ingestion process is called **landing**. Multiple teams produce data to Kafka topics. This data needs to be landed into the data platform in a queryable, governed, and scalable way.

Key constraints:

- Micro-batch interval: **30 seconds**
- Batch size: **< 1MB Parquet** per batch
- Many teams, many topics, one shared infrastructure
- Schema registry: **Apicurio** (BACKWARD compatibility enforced)
- Company stack: **Kotlin** (primary), Spark for heavy lifting
- Query engines: **Trino** (analytics), **DuckDB** (fresh/ad-hoc)

### Goals

- Absorb high-frequency, small-payload writes without Iceberg small-file problems
- Provide **sub-minute freshness** for recent data via DuckLake
- Provide **compacted, well-partitioned** historical data in Iceberg for Trino
- Support **schema evolution** governed by Apicurio without pipeline restarts
- Support **dead letter queues** at every failure stage
- Be operable by a Kotlin shop without deep Spark expertise for day-to-day work

### Non-Goals

- Sub-second streaming (Flink, Kafka Streams)
- OLTP-style point updates
- Replacing Trino or Spark as query engines

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│  Apicurio Schema Registry                                   │
│  (schema versions, BACKWARD compatibility enforced)         │
└───────────────────────┬─────────────────────────────────────┘
                        │ schema fetch on startup + change
                        │
┌───────────────────────▼─────────────────────────────────────┐
│  Team Pipelines — Duckling (Kotlin, one instance per team)  │
│                                                             │
│  team_a ──┐                                                 │
│  team_b ──┤──→ DuckDB process (per instance, embedded)      │
│  team_c ──┘         │                                       │
│               30s commits, <1MB Parquet each                │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│  DuckLake Store (shared across all teams)                   │
│                                                             │
│  ┌──────────────────────┐   ┌───────────────────────────┐  │
│  │ Postgres (catalog)   │   │ S3                        │  │
│  │  ducklake_tables     │   │  ducklake/team_a/orders/  │  │
│  │  ducklake_snapshots  │   │  ducklake/team_b/events/  │  │
│  │  ducklake_data_files │   │  ducklake/team_c/metrics/ │  │
│  │  ducklake_export_    │   │                           │  │
│  │    state  (custom)   │   │  (raw, <1MB Parquet each) │  │
│  └──────────────────────┘   └───────────────────────────┘  │
└──────────┬───────────────────────────────┬──────────────────┘
           │                               │
           ▼                               ▼
┌──────────────────┐          ┌────────────────────────────────┐
│  DuckDB queries  │          │  Spark Exporter                │
│  (fresh, <24h)   │          │  (scheduled every 5 min)       │
│                  │          │                                │
│  MotherDuck or   │          │  per executor:                 │
│  local DuckDB    │          │  own DuckDB process            │
│                  │          │  → reads Postgres catalog      │
│  sub-minute lag  │          │  → reads S3 Parquet directly   │
└──────────────────┘          │  → writes compacted Iceberg    │
                              │                                │
                              │  fully parallel, no shared     │
                              │  DuckDB process bottleneck     │
                              └───────────────┬────────────────┘
                                              │
                              ┌───────────────▼────────────────┐
                              │  Iceberg                       │
                              │                                │
                              │  ┌─────────────┐  ┌────────┐  │
                              │  │ REST catalog│  │   S3   │  │
                              │  │             │  │ 128MB  │  │
                              │  │             │  │Parquet │  │
                              │  └─────────────┘  └────────┘  │
                              └───────────────┬────────────────┘
                                              │
                                   ┌──────────┴──────────┐
                                   ▼                     ▼
                              Trino                  Spark
                           (analytics)              (ETL)
                           up to ~60min lag
```

---

## 3. Component Design

### 3.1 Kotlin sql-flow Pipeline

Each team deploys one Duckling pipeline instance per Kafka topic (or group of topics). The pipeline is configured via a type-safe Kotlin DSL.

#### Pipeline DSL

```kotlin
val pipeline = duckling {
    source {
        kafka {
            brokers = env("KAFKA_BROKERS")
            topic = env("INPUT_TOPIC")
            groupId = "duckling-team_a-orders"
            schemaRegistry = env("APICURIO_URL")
        }
    }
    handler {
        sql = """
            SELECT
                order_id,
                customer_id,
                region,
                amount,
                event_time
            FROM input
            WHERE amount > 0
        """
        batchSize = 1000
        flushEverySeconds = 30
    }
    sink {
        duckLake {
            catalog = env("DUCKLAKE_CATALOG_URL")
            table = "team_a.orders"
            exportToIceberg = true
            targetExportFileSizeMb = 128
            maxExportIntervalMinutes = 60
        }
    }
    dlq {
        topic = "team_a.orders.dlq"
        brokers = env("KAFKA_BROKERS")
    }
}
```

#### Internal Pipeline Flow

```
KafkaConsumer.poll()
    ↓
SchemaAwareDeserializer     ← fetch schema from Apicurio
    ↓ (failure → DLQ)
Arrow batch builder         ← JSON bytes → Arrow columnar (off-heap)
    ↓
DuckDB SQL transformation   ← Arrow → DuckDB (zero-copy via C Data Interface)
    ↓ (row failure → binary search isolation → bad rows to DLQ)
DuckLake writer             ← DuckDB → Parquet on S3, metadata in Postgres
    ↓ (infra failure → retry + backoff, halt if exhausted)
Kafka offset commit         ← only after DLQ write confirmed (if applicable)
```

#### Zero-Copy Data Path

To minimize JVM overhead and GC pressure:

- Kafka bytes → Arrow `VectorSchemaRoot` using off-heap `RootAllocator` (Netty)
- Arrow → DuckDB via Arrow C Data Interface (no copy)
- DuckDB result → Arrow (no copy)
- Arrow buffers released immediately after DuckLake write via `close()`

#### Concurrency Model

Kotlin coroutines with `Channel` for pipeline stages:

```
Kafka poller (IO dispatcher)
    ↓ Channel<ConsumerRecord> (capacity 10_000)
Batch accumulator (Default dispatcher)
    ↓ Channel<VectorSchemaRoot> (capacity 4)
DuckDB handler (Default dispatcher)
    ↓ Channel<VectorSchemaRoot> (capacity 4)
DuckLake writer (IO dispatcher)
```

Backpressure is natural: if DuckLake is slow, channels fill and the Kafka poller pauses.

---

### 3.2 DuckLake Store

#### Shared Postgres Catalog

All teams share one Postgres instance as the DuckLake catalog. Postgres handles concurrent writes via standard database transactions — two writers to the same table serialize through Postgres, but this is rare since teams write to their own tables.

Custom table added to the catalog for export tracking:

```sql
CREATE TABLE ducklake_export_state (
    table_name              TEXT PRIMARY KEY,
    last_exported_snapshot  BIGINT,
    last_export_time        TIMESTAMPTZ,
    export_enabled          BOOLEAN DEFAULT true,
    export_blocked          BOOLEAN DEFAULT false,
    export_blocked_reason   TEXT
);
```

#### S3 Layout

```
s3://company-ducklake/
└── {team}/{table_name}/
    ├── ducklake-{uuid}.parquet     (~0.5MB, one per 30s batch)
    ├── ducklake-{uuid}.parquet
    └── ...
```

#### Retention Policy

DuckLake holds the last **24 hours** of snapshots for fresh querying. After export to Iceberg, old snapshots are expired and files cleaned up:

```sql
CALL ducklake_expire_snapshots(
    'ducklake',
    older_than => NOW() - INTERVAL '24 hours'
);
CALL ducklake_cleanup('ducklake');
```

#### Multiple Writers — No Bottleneck

Each Duckling pipeline instance runs its own embedded DuckDB process. Multiple processes connect independently to the shared Postgres catalog and shared S3. Reads are fully parallel. Writes serialize per-table through Postgres transactions only.

---

### 3.3 Spark Exporter

The Spark exporter runs as a scheduled Spark job (every 5 minutes). It discovers tables ready for export, reads their Parquet files from S3, and writes compacted data to Iceberg.

#### Export Trigger Logic

A table is ready for export when either:
- Accumulated unexported bytes ≥ 128MB (target Iceberg file size), OR
- Time since last export ≥ 60 minutes (ensures at most 1h Iceberg lag)

```python
# Query Postgres directly for ready tables
SELECT
    f.table_name,
    e.last_exported_snapshot,
    MAX(s.snapshot_id)         AS current_snapshot,
    SUM(f.file_size_bytes)     AS accumulated_bytes
FROM ducklake_data_files f
JOIN ducklake_snapshots s USING (snapshot_id)
LEFT JOIN ducklake_export_state e USING (table_name)
WHERE
    (e.last_exported_snapshot IS NULL
     OR s.snapshot_id > e.last_exported_snapshot)
AND COALESCE(e.export_enabled, true) = true
AND COALESCE(e.export_blocked, false) = false
GROUP BY f.table_name, e.last_exported_snapshot
HAVING
    SUM(f.file_size_bytes) >= 128 * 1024 * 1024
    OR EXTRACT(EPOCH FROM NOW() - MIN(s.commit_time)) / 60 >= 60
```

#### Per-Executor Export

Each Spark executor runs its own DuckDB process — no shared DuckDB instance, no bottleneck:

```python
def export_table(candidate):
    # Each executor: own DuckDB process
    conn = duckdb.connect()
    conn.execute("ATTACH 'ducklake:postgres:...' AS ducklake (READ_ONLY)")

    # Get file paths for unexported snapshots
    files = conn.execute(f"""
        SELECT file_path FROM ducklake_data_files
        WHERE table_name = '{candidate.table_name}'
          AND snapshot_id > {candidate.last_exported_snapshot or 0}
    """).fetchall()

    # Spark reads Parquet directly from S3 — fully distributed
    df = spark.read \
        .option("mergeSchema", "true") \  # handles schema evolution across files
        .parquet(*[f[0] for f in files])

    # Write to Iceberg — compacted, properly partitioned
    df.writeTo(f"iceberg.{candidate.table_name}") \
      .option("write.target-file-size-bytes", str(128 * 1024 * 1024)) \
      .append()

# All tables exported in parallel across Spark executors
spark.sparkContext \
    .parallelize(candidates) \
    .foreach(export_table)
```

#### DuckLake ↔ Iceberg Interoperability

DuckLake 0.3 supports metadata-only migration (no data copy):

```sql
-- Register existing DuckLake Parquet files with Iceberg catalog
-- No S3 data movement — files are already in the right place
CALL ducklake_to_iceberg('ducklake', 'iceberg_catalog',
    table_name => 'team_a.orders',
    up_to_snapshot => {snapshot_id}
);
```

---

### 3.4 Iceberg

Iceberg stores the full historical record, compacted into 128MB Parquet files, partitioned by time.

#### Why Not Write Directly to Iceberg

At 30-second intervals with <1MB payloads, writing directly to Iceberg generates:
- 2,880 commits/day
- 2,880 tiny Parquet files/day
- Equivalent metadata file accumulation

This requires constant compaction to keep query performance acceptable. DuckLake absorbs the commit storm; Iceberg receives only properly-sized files.

#### Schema

Iceberg tracks columns by **field ID** (not name), so renames don't break reads of old files. Schema evolution is applied by the Spark exporter based on the latest Apicurio schema before each write.

---

### 3.5 Query Layer

Two independent query paths:

| | DuckDB / MotherDuck | Trino |
|---|---|---|
| **Data source** | DuckLake | Iceberg |
| **Freshness** | Sub-minute | Up to ~60 min |
| **Best for** | Recent data, ad-hoc, per-team | Historical analytics, cross-team joins |
| **Retention** | Last 24 hours | Full history |

---

## 4. Schema Evolution

Apicurio is the **single source of truth** for schema compatibility. The pipeline never re-implements compatibility checks — it trusts that any schema version registered in Apicurio is compatible with the previous one.

### Compatibility Mode

All topics use **BACKWARD** compatibility:
- Add optional field with default ✅
- Remove field ✅
- Widen type (int → long) ✅
- Add required field without default ❌ (rejected by Apicurio)
- Narrow type ❌ (rejected by Apicurio)

### Evolution at Each Layer

#### Kafka → Kotlin (deserialization)

Avro's schema resolution handles field mapping automatically. The `GenericRecord` received by the pipeline always reflects the latest registered schema — missing fields get defaults, removed fields are dropped.

#### DuckLake (ALTER TABLE)

When the Kotlin pipeline detects schema drift between the latest Apicurio schema and the current DuckLake table schema:

```kotlin
diff.addedColumns.forEach { col ->
    conn.execute("""
        ALTER TABLE $tableName
        ADD COLUMN IF NOT EXISTS ${col.name} ${col.duckDbType}
        DEFAULT ${col.defaultValue}
    """)
}
// Removed columns: NOT dropped — old Parquet files still contain them.
// Old files are read correctly; new files simply omit the column.

diff.widenedColumns.forEach { col ->
    conn.execute("""
        ALTER TABLE $tableName
        ALTER COLUMN ${col.name} TYPE ${col.newDuckDbType}
    """)
}
```

#### Parquet files (no action required)

Parquet is self-describing. Old files with removed columns are still readable — DuckDB and Spark simply ignore columns not in the current schema. New files missing columns added later return null for those columns in old files.

#### Iceberg (update_schema)

Before each export, the Spark exporter syncs the Iceberg schema to the latest Apicurio schema:

```python
with iceberg_table.update_schema() as update:
    for col in diff.added_columns:
        update.add_column(col.name, col.iceberg_type)
    for col in diff.widened_columns:
        update.update_column(col.name, col.new_iceberg_type)
    for col in diff.renamed_columns:
        update.rename_column(col.old_name, col.new_name)
    # Removed columns: never dropped from Iceberg
    # Old files still physically contain them
```

### Rename Handling

Renames use Avro `aliases` to signal the mapping:

```json
{
  "name": "customer_id",
  "type": "string",
  "aliases": ["user_id"]
}
```

In DuckLake: add new column, backfill from old column, keep old column.
In Iceberg: use `rename_column` which preserves field IDs — old files remain readable.

### Schema Evolution Failure

If schema evolution cannot be auto-resolved (e.g. conflicting types), the Spark exporter:
1. Sends a DLQ event to `{table}.export.dlq`
2. Marks the table as `export_blocked = true` in `ducklake_export_state`
3. Continues exporting all other tables
4. Alerts on-call (via Prometheus metric `duckling_export_blocked_tables`)

---

## 5. Dead Letter Queue

### Failure Classification

| Failure | Stage | Action |
|---|---|---|
| Bad bytes / schema not in Apicurio | Deserialization | → DLQ, continue |
| Row fails SQL transformation | SQL | Binary search, bad rows → DLQ, good rows continue |
| DuckLake write (infra) | DuckLake write | Retry + backoff, halt pipeline if exhausted |
| Schema drift unresolvable | Spark export | → Export DLQ, block table |
| Corrupt Parquet file | Spark export | → Export DLQ, skip file, continue |
| Iceberg / S3 unavailable | Spark export | Spark retry (automatic) |

### DLQ Message Format

JSON (not Avro — must be readable even when schema registry is unavailable):

```json
{
  "original_topic": "orders",
  "original_partition": 3,
  "original_offset": 104521,
  "original_key": "order-123",
  "original_payload": "<base64>",
  "original_headers": {},
  "failure_stage": "DESERIALIZATION",
  "failure_reason": "Schema ID 42 not found in registry",
  "failure_stack_trace": "...",
  "failure_time": "2025-03-29T10:00:00Z",
  "schema_id": 42,
  "schema_version": null,
  "pipeline_name": "team_a-orders",
  "team_name": "team_a"
}
```

### DLQ Topic Naming

```
{team}.{source_topic}.dlq          # deserialization / transformation failures
{team}.{table_name}.export.dlq     # export failures (Spark stage)
```

### Offset Commit Guarantee

Kafka offsets are committed **only after**:
- Successful DuckLake write, OR
- Successful DLQ write (for failed messages)

If the DLQ write itself fails, the pipeline pauses and retries — no silent message loss.

### Bad Row Isolation (Binary Search)

When a batch fails SQL transformation, rather than sending the entire batch to DLQ, Duckling binary-searches for the bad rows:

```
Batch of 1000 fails
  → Try first 500  → succeeds → commit
  → Try second 500 → fails
    → Try first 250 → succeeds → commit
    → Try second 250 → fails
      → ... recurse until single bad row identified → DLQ
```

This minimizes data loss: only truly bad rows go to DLQ.

### Replay

A `DlqReplayService` re-injects original bytes back into the source topic after the root cause is fixed:

```kotlin
replayService.replayTopic(
    dlqTopic = "team_a.orders.dlq",
    filter = { msg ->
        msg.failureStage == FailureStage.DESERIALIZATION &&
        msg.failureTime.isAfter(Instant.now().minus(1, ChronoUnit.HOURS))
    }
)
```

Replayed messages carry additional headers:
- `dlq.replay.time`
- `dlq.original.failure`

---

## 6. Configuration DSL

Duckling uses a type-safe Kotlin DSL instead of YAML. This gives teams IDE autocomplete, compile-time validation, and the ability to use environment variables, conditionals, and shared config fragments.

```kotlin
// Shared config fragment — reusable across teams
fun standardKafkaSource(topic: String) = kafkaSource {
    brokers = env("KAFKA_BROKERS")
    groupId = "duckling-$topic"
    schemaRegistry = env("APICURIO_URL")
    this.topic = topic
}

// Team pipeline definition
val pipeline = duckling {
    source { standardKafkaSource("team_a.orders") }

    handler {
        sql = """
            SELECT order_id, region, amount, event_time
            FROM input WHERE amount > 0
        """
        batchSize = 1000
        flushEverySeconds = 30
    }

    sink {
        duckLake {
            catalog = env("DUCKLAKE_CATALOG_URL")
            table = "team_a.orders"
            exportToIceberg = true
            targetExportFileSizeMb = 128
            maxExportIntervalMinutes = 60
        }
    }

    dlq {
        topic = "team_a.orders.dlq"
        brokers = env("KAFKA_BROKERS")
    }

    // Conditionally add monitoring sink in production
    if (env("ENV") == "production") {
        metrics { prometheus { port = 9090 } }
    }
}
```

The DSL is implemented using Kotlin's type-safe builder pattern with `@DslMarker` to prevent accidental scope leakage.

---

## 7. Performance Considerations

### JVM Overhead Mitigation

| Technique | Benefit |
|---|---|
| Arrow off-heap allocator (Netty) | Data buffers outside JVM heap — GC never sees them |
| Arrow C Data Interface to DuckDB | Zero-copy Kafka→DuckLake hot path |
| Channel-based pipeline | Backpressure without thread-per-message |
| `close()` after every batch | Off-heap memory released immediately |

### Kafka Client

The Java Kafka client outperforms confluent-kafka-python in throughput benchmarks (~3x on single CPU) primarily because Python's GIL throttles the application side. The Java client also combines batches across partitions into single produce requests, which librdkafka cannot do.

### DuckLake Write Cost

Each 30-second commit is:
- A few Parquet file writes to S3 (<1MB)
- A few SQL row inserts into Postgres (microseconds)
- No manifest files, no JSON metadata on S3

Compare to Iceberg: each commit writes manifest files + snapshot JSON to S3 in addition to Parquet data.

### Spark Exporter Parallelism

- One Spark executor per table being exported
- Each executor has its own DuckDB process
- No shared DuckDB process bottleneck
- S3 reads are fully parallel across executors
- Concurrency limited by Spark cluster size, not DuckDB

---

## 8. Project Structure

```
duckling/
├── build.gradle.kts                    # root build
├── settings.gradle.kts
│
├── duckling-core/                      # shared models, interfaces, DSL
│   └── src/main/kotlin/
│       └── io/company/duckling/
│           ├── config/
│           │   ├── PipelineConfig.kt
│           │   ├── SourceConfig.kt
│           │   ├── SinkConfig.kt
│           │   └── DslBuilder.kt       # type-safe DSL
│           ├── model/
│           │   ├── DlqMessage.kt
│           │   └── ProcessResult.kt
│           └── arrow/
│               └── ArrowAllocator.kt   # shared off-heap allocator
│
├── duckling-source/                    # Kafka source
│   └── src/main/kotlin/
│       └── io/company/duckling/source/
│           ├── KafkaSource.kt
│           └── SchemaAwareDeserializer.kt
│
├── duckling-handler/                   # DuckDB SQL transformation
│   └── src/main/kotlin/
│       └── io/company/duckling/handler/
│           ├── DuckDBHandler.kt
│           └── BadRowIsolator.kt       # binary search for DLQ
│
├── duckling-sink/                      # DuckLake + DLQ writers
│   └── src/main/kotlin/
│       └── io/company/duckling/sink/
│           ├── DuckLakeWriter.kt
│           ├── SchemaEvolution.kt      # ALTER TABLE logic
│           └── DlqWriter.kt
│
├── duckling-pipeline/                  # pipeline orchestration
│   └── src/main/kotlin/
│       └── io/company/duckling/
│           ├── Pipeline.kt             # coroutine pipeline runner
│           └── Main.kt
│
├── duckling-exporter/                  # Spark export job (Python)
│   ├── exporter.py                     # main Spark job
│   ├── schema_sync.py                  # Apicurio → Iceberg schema evolution
│   ├── dlq.py                          # export DLQ events
│   └── requirements.txt
│
└── duckling-replay/                    # DLQ replay utility
    └── src/main/kotlin/
        └── io/company/duckling/replay/
            └── DlqReplayService.kt
```

---

## 9. Technology Stack

| Component | Technology | Notes |
|---|---|---|
| Language (pipeline) | Kotlin + Coroutines | Company standard |
| Language (exporter) | PySpark | Spark ecosystem |
| Kafka client | kafka-clients (Java) | ~3x faster than confluent-kafka-python |
| Schema registry | Apicurio | BACKWARD compatibility enforced |
| Serialization format | Avro (Kafka) | Parquet (storage) |
| Data buffer | Arrow (off-heap) | Netty allocator, zero-copy |
| Stream processor | DuckDB (embedded) | JVM in-process via JDBC |
| Buffer store | DuckLake | Postgres catalog + S3 Parquet |
| Catalog (DuckLake) | PostgreSQL | Shared across teams |
| Catalog (Iceberg) | Iceberg REST catalog | Standard |
| Object storage | S3 | Both DuckLake and Iceberg |
| Analytical store | Apache Iceberg | 128MB files, partitioned |
| Batch processor | Apache Spark 4.0+ | Export job only |
| Fresh query engine | DuckDB / MotherDuck | Last 24h, sub-minute lag |
| Analytics query engine | Trino | Historical, cross-team |
| Metrics | Micrometer + Prometheus | Standard JVM metrics |
| Config | Kotlin DSL | Type-safe, no YAML |
| Build | Gradle (Kotlin DSL) | Company standard |

### Key Library Versions

```kotlin
// build.gradle.kts
val kafkaVersion = "3.7.0"
val arrowVersion = "14.0.0"
val duckdbVersion = "1.1.3"
val icebergVersion = "1.4.3"
val coroutinesVersion = "1.8.0"
val apicurioVersion = "2.5.0"
```

---

## 10. Open Questions

### Infrastructure

- [ ] Who owns the shared Postgres catalog? Platform team or each team?
- [ ] DuckLake catalog HA strategy — Postgres failover, read replicas?
- [ ] S3 bucket layout — one bucket for DuckLake + Iceberg, or separate?
- [ ] MotherDuck vs self-hosted DuckDB for fresh query layer?

### Operational

- [ ] How are teams onboarded? Self-service registration or platform team approval?
- [ ] Export blocked tables — automated alerting owner? Who resolves schema conflicts?
- [ ] DLQ retention period — how long before DLQ messages are dropped?
- [ ] Who is responsible for DLQ replay — teams or platform team?

### Schema Evolution

- [ ] Renames — handled via Avro aliases, but requires producer discipline. Enforce in Apicurio linting?
- [ ] What happens if a team registers a schema with NONE compatibility? Block at pipeline level?

### Performance

- [ ] Spark exporter cluster sizing — how many executors for N teams?
- [ ] DuckLake 24h retention — correct for all teams, or configurable per table?
- [ ] Export interval floor — 60 minutes acceptable for all Trino consumers?

### Future

- [ ] Native Spark DuckLake connector (ducklake-spark) maturity — migrate when stable
- [ ] Trino DuckLake connector — tracking GitHub issue #26523
- [ ] WebSocket sources (Bluesky-style) — same pipeline, different source module
