"""
Syncs an Iceberg table schema to match the latest Apicurio Avro schema.
Only safe evolutions are applied: add nullable column, widen BIGINT→DOUBLE.
Removed columns are never dropped from Iceberg.
"""
import requests
from pyiceberg.types import (
    StringType, LongType, DoubleType, BooleanType,
    NestedField,
)


AVRO_TO_ICEBERG = {
    "string":  StringType(),
    "int":     LongType(),
    "long":    LongType(),
    "float":   DoubleType(),
    "double":  DoubleType(),
    "boolean": BooleanType(),
}


def _avro_field_to_iceberg(field: dict) -> tuple[str, object, bool]:
    """Returns (name, iceberg_type, nullable)."""
    name = field["name"]
    schema = field["type"]

    if isinstance(schema, str):
        iceberg_type = AVRO_TO_ICEBERG.get(schema)
        if iceberg_type is None:
            raise ValueError(f"Unsupported Avro type '{schema}' for field '{name}'")
        return name, iceberg_type, False

    if isinstance(schema, list):
        # union — expect [null, T]
        non_null = [t for t in schema if t != "null"]
        if len(non_null) != 1:
            raise ValueError(f"Field '{name}' has unsupported union: {schema}")
        iceberg_type = AVRO_TO_ICEBERG.get(non_null[0])
        if iceberg_type is None:
            raise ValueError(f"Unsupported Avro type '{non_null[0]}' for field '{name}'")
        return name, iceberg_type, True

    raise ValueError(f"Field '{name}' has unsupported complex Avro type: {schema}")


def fetch_avro_schema(registry_url: str, group_id: str, artifact_id: str) -> dict:
    url = f"{registry_url}/groups/{group_id}/artifacts/{artifact_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def sync_iceberg_schema(
    iceberg_catalog,
    iceberg_table_name: str,
    registry_url: str,
    group_id: str,
    artifact_id: str,
):
    """
    Fetches the latest Avro schema from Apicurio and applies safe DDL
    to the Iceberg table. Skips if the table does not yet exist.
    """
    avro_schema = fetch_avro_schema(registry_url, group_id, artifact_id)
    avro_fields = avro_schema.get("fields", [])

    try:
        table = iceberg_catalog.load_table(iceberg_table_name)
    except Exception:
        # Table doesn't exist yet; it will be created on first write
        return

    current_fields = {f.name: f for f in table.schema().fields}
    new_field_specs = [_avro_field_to_iceberg(f) for f in avro_fields]

    added = [(n, t, nullable) for n, t, nullable in new_field_specs if n not in current_fields]
    widened = []
    for name, iceberg_type, _ in new_field_specs:
        if name not in current_fields:
            continue
        current = current_fields[name].field_type
        if type(current) != type(iceberg_type):
            if isinstance(current, LongType) and isinstance(iceberg_type, DoubleType):
                widened.append((name, iceberg_type))
            else:
                raise ValueError(
                    f"Non-widening type change on column '{name}': "
                    f"{current} → {iceberg_type}"
                )

    if not added and not widened:
        return

    with table.update_schema() as update:
        field_id = max((f.field_id for f in table.schema().fields), default=0)
        for name, iceberg_type, nullable in added:
            field_id += 1
            update.add_column(
                name,
                iceberg_type,
                required=not nullable,
            )
        for name, iceberg_type in widened:
            update.update_column(name, iceberg_type)
