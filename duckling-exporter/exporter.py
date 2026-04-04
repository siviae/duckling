"""
Duckling Exporter
=================
Registers existing DuckLake Parquet files directly with Iceberg (Lakekeeper REST
catalog) via add_files — no data is read or re-written.

Run:
    uv run exporter.py
"""
import json
import os
import duckdb
import psycopg2
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg import types as T
from pyiceberg.schema import Schema, NestedField

DUCKLAKE_CATALOG_URL = os.environ["DUCKLAKE_CATALOG_POSTGRES_URL"]
ICEBERG_REST_URL = os.environ.get("ICEBERG_REST_URL", "http://lakekeeper:8181")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://garage:3900")
AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

# DuckDB column_type → Iceberg type (bare type name, before any '(')
_DUCKDB_TO_ICEBERG: dict[str, T.IcebergType] = {
    "VARCHAR":   T.StringType(),
    "TEXT":      T.StringType(),
    "BIGINT":    T.LongType(),
    "HUGEINT":   T.LongType(),
    "INTEGER":   T.IntegerType(),
    "INT":       T.IntegerType(),
    "SMALLINT":  T.IntegerType(),
    "TINYINT":   T.IntegerType(),
    "DOUBLE":    T.DoubleType(),
    "FLOAT":     T.FloatType(),
    "BOOLEAN":   T.BooleanType(),
    "BOOL":      T.BooleanType(),
    "DATE":      T.DateType(),
    "TIMESTAMP": T.TimestampType(),
    "BLOB":      T.BinaryType(),
}


def _parse_jdbc_url(jdbc_url: str) -> dict:
    without_prefix = jdbc_url.removeprefix("jdbc:postgresql://")
    host_part, rest = without_prefix.split("/", 1)
    host = host_part.split(":")[0]
    port = int(host_part.split(":")[1]) if ":" in host_part else 5432
    db_and_params = rest.split("?", 1)
    dbname = db_and_params[0]
    params: dict[str, str] = {}
    if len(db_and_params) > 1:
        for kv in db_and_params[1].split("&"):
            if "=" in kv:
                k, v = kv.split("=", 1)
                params[k] = v
    return {"host": host, "port": port, "dbname": dbname,
            "user": params.get("user", "duckling"),
            "password": params.get("password", "duckling")}


def _ducklake_attach_string(p: dict) -> str:
    return (f"ducklake:postgres:host={p['host']} port={p['port']} "
            f"dbname={p['dbname']} user={p['user']} password={p['password']}")


def _duckdb_type_to_iceberg(column_type: str) -> T.IcebergType:
    base = column_type.upper().split("(")[0].strip()
    return _DUCKDB_TO_ICEBERG.get(base, T.StringType())


def main() -> None:
    pg_params = _parse_jdbc_url(DUCKLAKE_CATALOG_URL)

    # DuckDB connection — used only for schema discovery (DESCRIBE)
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake")
    endpoint = S3_ENDPOINT.removeprefix("http://").removeprefix("https://")
    use_ssl = S3_ENDPOINT.startswith("https://")
    conn.execute(f"""
        CREATE SECRET duckling_s3 (
            TYPE S3,
            KEY_ID '{AWS_ACCESS_KEY}',
            SECRET '{AWS_SECRET_KEY}',
            ENDPOINT '{endpoint}',
            USE_SSL {str(use_ssl).lower()},
            URL_STYLE 'path',
            REGION 'garage'
        )
    """)
    conn.execute(f"ATTACH '{_ducklake_attach_string(pg_params)}' AS ducklake")

    # Postgres connection — used to read DuckLake catalog (file paths)
    pg = psycopg2.connect(**pg_params)

    catalog = RestCatalog(
        name="landing",
        **{
            "uri": f"{ICEBERG_REST_URL}/catalog",
            "warehouse": "landing",
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
            "s3.path-style-access": "true",
        },
    )

    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = 'ducklake' AND table_schema = 'main'"
    ).fetchall()
    print(f"[export] Found {len(tables)} table(s) in DuckLake")

    for (table_name,) in tables:
        try:
            # Get S3 Parquet file paths from DuckLake Postgres catalog
            with pg.cursor() as cur:
                cur.execute("""
                    SELECT df.file_path
                    FROM ducklake_data_file df
                    JOIN ducklake_table dt ON dt.table_id = df.table_id
                    WHERE dt.table_name = %s
                """, (table_name,))
                file_paths = [row[0] for row in cur.fetchall()]

            if not file_paths:
                print(f"[export] No Parquet files for {table_name}, skipping")
                continue
            print(f"[export] {table_name}: {len(file_paths)} file(s)")

            # Build Iceberg schema from DuckDB DESCRIBE (no data read)
            columns = conn.execute(f'DESCRIBE ducklake.main."{table_name}"').fetchall()
            iceberg_fields = [
                NestedField(
                    field_id=i + 1,
                    name=col[0],
                    field_type=_duckdb_type_to_iceberg(col[1]),
                    required=(col[2] == "NO"),
                )
                for i, col in enumerate(columns)
            ]
            iceberg_schema = Schema(*iceberg_fields)

            # Name mapping lets Iceberg readers resolve columns by name since
            # DuckLake Parquet files have no embedded Iceberg field IDs.
            name_mapping = json.dumps([
                {"field-id": f.field_id, "names": [f.name]}
                for f in iceberg_schema.fields
            ])

            ns = "landing"
            try:
                catalog.drop_table((ns, table_name))
            except NoSuchTableError:
                pass
            tbl = catalog.create_table(
                (ns, table_name),
                schema=iceberg_schema,
                properties={"schema.name-mapping.default": name_mapping},
            )
            tbl.add_files(file_paths)
            print(f"[export] Registered {len(file_paths)} file(s) → {ns}.{table_name}")
        except Exception as e:
            print(f"[export] ERROR exporting {table_name}: {e}")
            raise

    conn.close()
    pg.close()
    print("[export] Done")


if __name__ == "__main__":
    main()
