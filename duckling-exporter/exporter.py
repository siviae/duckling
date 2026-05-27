#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb>=1.5.1",
#     "psycopg2-binary>=2.9",
#     "pyiceberg[pyarrow,s3fs]>=0.9.0",
#     "pyarrow>=14.0.0",
# ]
# ///
"""
Duckling Exporter
=================
Registers existing DuckLake Parquet files with Iceberg (Lakekeeper REST catalog)
via pure metadata manipulation — no S3 data reads, zero data copy.

Strategy:
  1. DuckLake Postgres  → file paths, record counts, file sizes   (no S3)
  2. DuckDB DESCRIBE    → Iceberg schema                          (DuckDB reads S3)
  3. PyIceberg          → load or create Iceberg table            (idempotent)
  4. fast_append        → write manifest metadata only            (small S3 write)

Run:
    ./exporter.py
    uv run exporter.py
"""
import json
import os
import re
import duckdb
import psycopg2
import pyarrow.fs as pafs
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io import load_file_io as _pyiceberg_load_file_io
from pyiceberg import types as T
from pyiceberg.schema import Schema, NestedField
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.typedef import Record

# Lakekeeper returns server-side REST signer config and py-io-impl overrides in every
# table response.  Patch _load_file_io so that explicitly-set catalog-level properties
# (py-io-impl, s3.endpoint) survive the merge, and REST signer config is stripped when
# direct S3 credentials are present so that PyArrowFileIO can authenticate directly.
def _patched_load_file_io(self, properties=None, location=None):
    merged = {**self.properties, **(properties or {})}
    # Restore s3.endpoint so the Docker-internal hostname returned by Lakekeeper
    # doesn't override the explicitly-configured catalog endpoint.
    if "s3.endpoint" in self.properties:
        merged["s3.endpoint"] = self.properties["s3.endpoint"]
    # Strip REST signer config so FsspecFileIO uses direct aiobotocore SigV4 signing,
    # which is compatible with Garage (unlike Lakekeeper's REST signer which requires
    # a token we don't have).
    if "s3.access-key-id" in self.properties:
        for k in [k for k in merged if k.startswith("s3.signer")]:
            del merged[k]
    return _pyiceberg_load_file_io(merged, location)

RestCatalog._load_file_io = _patched_load_file_io

DUCKLAKE_CATALOG_URL = os.environ["DUCKLAKE_CATALOG_POSTGRES_URL"]
ICEBERG_REST_URL = os.environ.get("ICEBERG_REST_URL", "http://lakekeeper:8181")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "landing")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://garage:3900")
S3_REGION = os.environ.get("AWS_DEFAULT_REGION", "garage")
AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]


def _parse_duckdb_type(col_type: str) -> T.IcebergType:
    """Map a DuckDB column type string to the closest Iceberg type."""
    t = col_type.strip()
    up = t.upper()

    # DECIMAL(precision, scale)
    m = re.match(r"(?:DECIMAL|NUMERIC)\((\d+),\s*(\d+)\)", up)
    if m:
        return T.DecimalType(precision=int(m.group(1)), scale=int(m.group(2)))
    if up in ("DECIMAL", "NUMERIC"):
        return T.DecimalType(precision=38, scale=10)

    # TIMESTAMP WITH TIME ZONE
    if up in ("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", "TIMESTAMP_TZ"):
        return T.TimestamptzType()

    # LIST: element_type[]
    if t.endswith("[]"):
        elem = _parse_duckdb_type(t[:-2])
        return T.ListType(element_id=1, element_type=elem, element_required=False)

    # STRUCT / MAP — fall back to STRING (no lossless Iceberg equivalent via name-only)
    if up.startswith("STRUCT(") or up.startswith("MAP("):
        print(f"[export] WARNING: nested type '{col_type}' mapped to STRING — "
              "consider manual schema override")
        return T.StringType()

    SIMPLE: dict[str, T.IcebergType] = {
        # String
        "VARCHAR": T.StringType(), "TEXT": T.StringType(), "STRING": T.StringType(),
        "JSON": T.StringType(), "INTERVAL": T.StringType(),
        # UUID
        "UUID": T.UUIDType(),
        # 64-bit integer
        "BIGINT": T.LongType(), "INT8": T.LongType(), "LONG": T.LongType(),
        "UBIGINT": T.LongType(), "UINTEGER": T.LongType(),
        # 128-bit integer — no Iceberg equivalent; DECIMAL(38,0) preserves range
        "HUGEINT": T.DecimalType(precision=38, scale=0),
        "UHUGEINT": T.DecimalType(precision=39, scale=0),
        # 32-bit integer
        "INTEGER": T.IntegerType(), "INT": T.IntegerType(), "INT4": T.IntegerType(),
        "SIGNED": T.IntegerType(),
        # 16-bit and 8-bit — Iceberg has no SMALLINT/TINYINT; INT covers them
        "SMALLINT": T.IntegerType(), "INT2": T.IntegerType(), "SHORT": T.IntegerType(),
        "USMALLINT": T.IntegerType(),
        "TINYINT": T.IntegerType(), "INT1": T.IntegerType(), "UTINYINT": T.IntegerType(),
        # Float
        "DOUBLE": T.DoubleType(), "FLOAT8": T.DoubleType(),
        "FLOAT": T.FloatType(), "FLOAT4": T.FloatType(), "REAL": T.FloatType(),
        # Boolean
        "BOOLEAN": T.BooleanType(), "BOOL": T.BooleanType(), "LOGICAL": T.BooleanType(),
        # Date/time
        "DATE": T.DateType(),
        "TIMESTAMP": T.TimestampType(), "DATETIME": T.TimestampType(),
        "TIMESTAMP_S": T.TimestampType(), "TIMESTAMP_MS": T.TimestampType(),
        "TIMESTAMP_NS": T.TimestampType(),
        "TIME": T.TimeType(),
        # Binary
        "BLOB": T.BinaryType(), "BYTEA": T.BinaryType(),
        "BINARY": T.BinaryType(), "VARBINARY": T.BinaryType(),
    }
    result = SIMPLE.get(up)
    if result is None:
        print(f"[export] WARNING: unknown DuckDB type '{col_type}', mapping to STRING")
        return T.StringType()
    return result


def _build_iceberg_schema(columns: list) -> tuple[Schema, str]:
    """
    Build an Iceberg Schema + name-mapping JSON from DuckDB DESCRIBE rows.
    Field IDs start at 1 and are assigned in column order — stable as long as
    the table is never recreated (we load existing tables instead of dropping).
    """
    fields = [
        NestedField(
            field_id=i + 1,
            name=col[0],
            field_type=_parse_duckdb_type(col[1]),
            required=(col[2] == "NO"),
        )
        for i, col in enumerate(columns)
    ]
    schema = Schema(*fields)
    # Name mapping lets readers resolve column names → field IDs since DuckLake
    # Parquet files carry no embedded Iceberg field IDs.
    name_mapping = json.dumps([
        {"field-id": f.field_id, "names": [f.name]}
        for f in schema.fields
    ])
    return schema, name_mapping


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


def _do_export(pg_params: dict, endpoint_no_scheme: str, use_ssl: bool,
               catalog: RestCatalog) -> None:
    """Creates fresh DuckDB/Postgres connections, runs the export, then closes them.
    catalog and S3 subsystem are already initialised by the caller."""
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake")
    conn.execute(f"""
        CREATE SECRET duckling_s3 (
            TYPE S3,
            KEY_ID '{AWS_ACCESS_KEY}',
            SECRET '{AWS_SECRET_KEY}',
            ENDPOINT '{endpoint_no_scheme}',
            USE_SSL {str(use_ssl).lower()},
            URL_STYLE 'path',
            REGION 'garage'
        )
    """)
    conn.execute(f"ATTACH '{_ducklake_attach_string(pg_params)}' AS ducklake (AUTOMATIC_MIGRATION TRUE)")

    pg = psycopg2.connect(**pg_params)

    try:
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = 'ducklake' AND table_schema = 'main'"
        ).fetchall()
        print(f"[export] Found {len(tables)} table(s) in DuckLake")

        ns = ICEBERG_WAREHOUSE
        for (table_name,) in tables:
            try:
                parts = table_name.split("__", 1)
                data_path = f"s3://{parts[0]}/ducklake/main/{table_name}/" if parts else ""

                with pg.cursor() as cur:
                    cur.execute("""
                        SELECT df.path, df.path_is_relative,
                               df.record_count, df.file_size_bytes
                        FROM ducklake_data_file df
                        WHERE df.table_id = (
                            SELECT MAX(table_id) FROM ducklake_table WHERE table_name = %s
                        )
                        AND df.end_snapshot IS NULL
                    """, (table_name,))
                    rows = cur.fetchall()

                if not rows:
                    print(f"[export] {table_name}: no Parquet files, skipping")
                    continue

                all_data_files: list[DataFile] = []
                for path, is_relative, record_count, file_size in rows:
                    full_path = (data_path + path) if is_relative and data_path else path
                    all_data_files.append(DataFile.from_args(
                        content=DataFileContent.DATA,
                        file_path=full_path,
                        file_format=FileFormat.PARQUET,
                        partition=Record(),
                        record_count=record_count,
                        file_size_in_bytes=file_size,
                    ))

                columns = conn.execute(f'DESCRIBE ducklake.main."{table_name}"').fetchall()

                try:
                    tbl = catalog.load_table((ns, table_name))
                    duckdb_cols = {col[0] for col in columns}
                    iceberg_cols = {f.name for f in tbl.schema().fields}
                    new_cols = duckdb_cols - iceberg_cols
                    gone_cols = iceberg_cols - duckdb_cols
                    if new_cols or gone_cols:
                        print(f"[export] WARNING {table_name}: schema drift — "
                              f"new: {new_cols or '∅'}, removed: {gone_cols or '∅'}")
                    print(f"[export] {table_name}: loaded existing table")
                except NoSuchTableError:
                    iceberg_schema, name_mapping = _build_iceberg_schema(columns)
                    tbl = catalog.create_table(
                        (ns, table_name),
                        schema=iceberg_schema,
                        properties={"schema.name-mapping.default": name_mapping},
                    )
                    print(f"[export] {table_name}: created Iceberg table")

                existing_paths: set[str] = set()
                if tbl.current_snapshot() is not None:
                    for task in tbl.scan().plan_files():
                        existing_paths.add(task.file.file_path)

                new_files = [df for df in all_data_files if df.file_path not in existing_paths]

                if not new_files:
                    print(f"[export] {table_name}: all {len(all_data_files)} file(s) already registered")
                    continue

                skipped = len(all_data_files) - len(new_files)
                if skipped:
                    print(f"[export] {table_name}: skipping {skipped} already-registered file(s)")

                with tbl.transaction() as tx:
                    with tx.update_snapshot().fast_append() as append:
                        for df in new_files:
                            append.append_data_file(df)

                total_rows = sum(df.record_count for df in new_files)
                print(f"[export] {table_name}: registered {len(new_files)} file(s), "
                      f"{total_rows} row(s) → {ns}.{table_name}")

            except Exception as e:
                print(f"[export] WARNING: skipping {table_name} due to error: {e}")
    finally:
        conn.close()
        pg.close()

    print("[export] Done")


def _build_export_context() -> tuple:
    """Performs one-time expensive initialization: S3 subsystem + Iceberg catalog."""
    endpoint_no_scheme = S3_ENDPOINT.removeprefix("http://").removeprefix("https://")
    use_ssl = S3_ENDPOINT.startswith("https://")
    pafs.initialize_s3(pafs.S3LogLevel.Fatal)
    # Pre-install DuckLake so INSTALL in _do_export() is a fast no-op (no network).
    _tmp = duckdb.connect()
    try:
        _tmp.execute("INSTALL ducklake")
    finally:
        _tmp.close()
    pg_params = _parse_jdbc_url(DUCKLAKE_CATALOG_URL)
    catalog = RestCatalog(
        name=ICEBERG_WAREHOUSE,
        **{
            "uri": f"{ICEBERG_REST_URL}/catalog",
            "warehouse": ICEBERG_WAREHOUSE,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
            "s3.region": S3_REGION,
        },
    )
    return pg_params, endpoint_no_scheme, use_ssl, catalog


def _serve() -> None:
    """Server mode: initialise once, then serve /export over HTTP.
    GET  /health → 200 ok  (readiness probe)
    POST /export → 200 output | 500 error message
    """
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import io as _io

    pg_params, endpoint_no_scheme, use_ssl, catalog = _build_export_context()
    port = int(os.environ.get("EXPORTER_PORT", "9091"))
    print(f"[export] Server ready on :{port}")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok\n")
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            if self.path == "/export":
                buf = _io.StringIO()
                import contextlib
                try:
                    with contextlib.redirect_stdout(buf):
                        _do_export(pg_params, endpoint_no_scheme, use_ssl, catalog)
                    body = buf.getvalue().encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain")
                    self.end_headers()
                    self.wfile.write(body)
                except Exception as e:
                    body = f"ERROR: {e}\n{buf.getvalue()}".encode()
                    self.send_response(500)
                    self.send_header("Content-Type", "text/plain")
                    self.end_headers()
                    self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt, *args):
            pass  # suppress per-request access logs

    HTTPServer(("0.0.0.0", port), Handler).serve_forever()


def main() -> None:
    if os.environ.get("UV_PREWARM"):
        print("[export] Prewarm: dependencies installed.")
        return

    if os.environ.get("EXPORTER_MODE") == "server":
        _serve()
        return

    pg_params, endpoint_no_scheme, use_ssl, catalog = _build_export_context()
    _do_export(pg_params, endpoint_no_scheme, use_ssl, catalog)


if __name__ == "__main__":
    main()
