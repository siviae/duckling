"""
Duckling Spark Exporter
=======================
Discovers DuckLake tables ready for export, reads their Parquet files from
MinIO, compacts them into 128MB Iceberg files, then marks the export complete
and triggers DuckLake cleanup.

Run every 5 minutes via cron / Kubernetes CronJob:
    uv run exporter.py
"""
import os
import duckdb
import psycopg2
from pyspark.sql import SparkSession
from pyiceberg.catalog.rest import RestCatalog
from schema_sync import sync_iceberg_schema

DUCKLAKE_CATALOG_URL = os.environ["DUCKLAKE_CATALOG_POSTGRES_URL"]
ICEBERG_REST_URL     = os.environ.get("ICEBERG_REST_URL", "http://lakekeeper:8181")
APICURIO_URL         = os.environ.get("APICURIO_URL", "http://apicurio:8080/apis/registry/v2")
S3_ENDPOINT          = os.environ.get("AWS_ENDPOINT_URL", "http://garage:3900")
AWS_ACCESS_KEY       = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY       = os.environ["AWS_SECRET_ACCESS_KEY"]

TARGET_FILE_BYTES    = 128 * 1024 * 1024  # 128 MB
MIN_EXPORT_BYTES     = int(os.environ.get("DUCKLING_MIN_EXPORT_BYTES",  str(128 * 1024 * 1024)))
MAX_EXPORT_AGE_MIN   = int(os.environ.get("DUCKLING_MAX_EXPORT_AGE_MIN", "60"))


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("duckling-exporter")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", ICEBERG_REST_URL)
        .config("spark.sql.catalog.iceberg.warehouse", "landing")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def find_export_candidates(pg_conn) -> list[dict]:
    """Returns tables ready for export per the design criteria."""
    query = """
        SELECT
            f.table_name,
            e.last_exported_snapshot,
            MAX(s.snapshot_id)     AS current_snapshot,
            SUM(f.file_size_bytes) AS accumulated_bytes
        FROM ducklake_data_files f
        JOIN ducklake_snapshots s USING (snapshot_id)
        LEFT JOIN ducklake_export_state e USING (table_name)
        WHERE
            (e.last_exported_snapshot IS NULL OR s.snapshot_id > e.last_exported_snapshot)
            AND COALESCE(e.export_enabled, true) = true
            AND COALESCE(e.export_blocked, false) = false
        GROUP BY f.table_name, e.last_exported_snapshot
        HAVING
            SUM(f.file_size_bytes) >= %s
            OR EXTRACT(EPOCH FROM NOW() - MIN(s.commit_time)) / 60 >= %s
    """
    with pg_conn.cursor() as cur:
        cur.execute(query, (MIN_EXPORT_BYTES, MAX_EXPORT_AGE_MIN))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_parquet_files(pg_conn, table_name: str, since_snapshot: int | None) -> list[str]:
    query = """
        SELECT DISTINCT f.file_path
        FROM ducklake_data_files f
        JOIN ducklake_snapshots s USING (snapshot_id)
        WHERE f.table_name = %s
          AND s.snapshot_id > %s
    """
    with pg_conn.cursor() as cur:
        cur.execute(query, (table_name, since_snapshot or 0))
        return [row[0] for row in cur.fetchall()]


def iceberg_table_name(ducklake_table: str) -> str:
    """Converts 'is_name.table_name' to Iceberg 'landing.is_name__table_name'."""
    return "landing." + ducklake_table.replace(".", "__")


def ensure_export_state_row(pg_conn, table_name: str):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ducklake_export_state (table_name)
            VALUES (%s)
            ON CONFLICT (table_name) DO NOTHING
            """,
            (table_name,),
        )
    pg_conn.commit()


def mark_exported_and_cleanup(pg_conn, ducklake_conn, table_name: str, snapshot_id: int):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ducklake_export_state
            SET last_exported_snapshot = %s,
                last_export_time = NOW()
            WHERE table_name = %s
            """,
            (snapshot_id, table_name),
        )
    pg_conn.commit()

    ducklake_conn.execute(
        "CALL ducklake_expire_snapshots('ducklake', up_to_snapshot => ?)",
        [snapshot_id],
    )
    ducklake_conn.execute("CALL ducklake_cleanup('ducklake')")


def export_table(
    candidate: dict,
    spark: SparkSession,
    pg_conn,
    ducklake_conn,
    iceberg_catalog: RestCatalog,
):
    table_name      = candidate["table_name"]
    last_exported   = candidate["last_exported_snapshot"]
    current_snap    = candidate["current_snapshot"]
    iceberg_name    = iceberg_table_name(table_name)

    print(f"[export] {table_name} → {iceberg_name} (snap {last_exported} → {current_snap})")

    parquet_files = get_parquet_files(pg_conn, table_name, last_exported)
    if not parquet_files:
        print(f"[export] No new files for {table_name}, skipping")
        return

    # Sync Iceberg schema to latest Apicurio schema
    is_name, tbl = table_name.split(".", 1) if "." in table_name else (table_name, table_name)
    try:
        sync_iceberg_schema(
            iceberg_catalog=iceberg_catalog,
            iceberg_table_name=iceberg_name,
            registry_url=APICURIO_URL,
            group_id=is_name,
            artifact_id=table_name,
        )
    except Exception as e:
        print(f"[export] Schema sync failed for {table_name}: {e} — skipping")
        return

    df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(*parquet_files)
    )

    (
        df.writeTo(f"iceberg.{iceberg_name}")
        .option("write.target-file-size-bytes", str(TARGET_FILE_BYTES))
        .append()
    )

    mark_exported_and_cleanup(pg_conn, ducklake_conn, table_name, current_snap)
    print(f"[export] Done: {table_name}")


def main():
    spark = build_spark()

    pg_conn = psycopg2.connect(
        # Convert jdbc:postgresql://... to psycopg2 DSN
        dsn=DUCKLAKE_CATALOG_URL.replace("jdbc:postgresql://", "postgresql://")
    )

    ducklake_conn = duckdb.connect()
    ducklake_conn.execute("INSTALL ducklake; LOAD ducklake")
    ducklake_conn.execute(
        f"ATTACH '{DUCKLAKE_CATALOG_URL.replace('jdbc:', '')}' AS ducklake (TYPE DUCKLAKE)"
    )

    iceberg_catalog = RestCatalog(
        name="landing",
        **{
            "uri": ICEBERG_REST_URL,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
            "s3.path-style-access": "true",
        },
    )

    candidates = find_export_candidates(pg_conn)
    print(f"[export] Found {len(candidates)} candidate table(s)")

    for candidate in candidates:
        ensure_export_state_row(pg_conn, candidate["table_name"])
        try:
            export_table(candidate, spark, pg_conn, ducklake_conn, iceberg_catalog)
        except Exception as e:
            print(f"[export] ERROR exporting {candidate['table_name']}: {e}")
            # Mark as blocked so we don't retry in a tight loop
            with pg_conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE ducklake_export_state
                    SET export_blocked = true, export_blocked_reason = %s
                    WHERE table_name = %s
                    """,
                    (str(e), candidate["table_name"]),
                )
            pg_conn.commit()

    ducklake_conn.close()
    pg_conn.close()
    spark.stop()


if __name__ == "__main__":
    main()
