package io.company.duckling.sink

import io.company.duckling.source.SchemaValidator.ColumnDef
import org.apache.avro.generic.GenericRecord
import java.sql.Connection
import java.sql.DriverManager

/**
 * Manages one DuckDB connection per topic. Writes batches of Avro GenericRecords
 * to DuckLake (Postgres catalog + MinIO Parquet files).
 *
 * DuckLake attach string format:
 *   ducklake:postgres://<host>/<db>?user=...&password=...
 * We convert a plain JDBC URL like:
 *   jdbc:postgresql://host:5432/db?user=u&password=p
 * to the DuckLake form expected by ATTACH.
 */
class DuckLakeWriter(
    catalogJdbcUrl: String,
    private val isName: String,
    private val tableName: String
) : AutoCloseable {

    private val conn: Connection
    private val fullTableName = "${isName}__${tableName.replace(".", "__")}"
    private val s3Path = "s3://$isName/ducklake/$tableName/"

    init {
        Class.forName("org.duckdb.DuckDBDriver")
        conn = DriverManager.getConnection("jdbc:duckdb:")

        conn.createStatement().use { stmt ->
            stmt.execute("INSTALL ducklake")
            stmt.execute("LOAD ducklake")

            val duckLakeUrl = jdbcToDuckLakeUrl(catalogJdbcUrl)
            stmt.execute("""
                ATTACH '$duckLakeUrl' AS ducklake (TYPE DUCKLAKE, DATA_PATH '$s3Path')
            """.trimIndent())
        }
    }

    /** Ensures the table exists with the given columns, applying evolution DDL as needed. */
    fun ensureTable(columns: List<ColumnDef>) {
        val currentCols = currentColumns()

        if (currentCols.isEmpty()) {
            createTable(columns)
        } else {
            val diff = SchemaEvolution.compute(currentCols, columns)
            applyDiff(diff)
        }
    }

    /** Writes a batch of records. Columns are derived from the validated schema. */
    fun writeBatch(records: List<GenericRecord>, columns: List<ColumnDef>) {
        if (records.isEmpty()) return

        val colNames = columns.joinToString(", ") { it.name }
        val placeholders = columns.joinToString(", ") { "?" }
        val sql = "INSERT INTO ducklake.$fullTableName ($colNames) VALUES ($placeholders)"

        conn.prepareStatement(sql).use { ps ->
            for (record in records) {
                for ((i, col) in columns.withIndex()) {
                    val value = record.get(col.name)
                    validateValue(col, value)
                    ps.setObject(i + 1, value)
                }
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    private fun validateValue(col: ColumnDef, value: Any?) {
        if (value == null) return
        when (col.duckType) {
            "DOUBLE" -> {
                val d = (value as? Number)?.toDouble()
                if (d != null) {
                    require(d.isFinite()) { "NaN or Infinity in column '${col.name}' is not permitted" }
                }
            }
            "BIGINT" -> {
                if (value is Number) {
                    val l = value.toLong()
                    // verify it's within BIGINT range (Long is always within Long range)
                    require(value !is Double || (value as Double).toLong() == l) {
                        "Float value in BIGINT column '${col.name}'"
                    }
                }
            }
        }
    }

    private fun createTable(columns: List<ColumnDef>) {
        val colDefs = columns.joinToString(", ") { col ->
            val nullability = if (col.nullable) "" else " NOT NULL"
            "${col.name} ${col.duckType}$nullability"
        }
        conn.createStatement().use { stmt ->
            stmt.execute("CREATE TABLE IF NOT EXISTS ducklake.$fullTableName ($colDefs)")
        }
    }

    private fun applyDiff(diff: SchemaEvolution.Diff) {
        conn.createStatement().use { stmt ->
            for (col in diff.addedColumns) {
                stmt.execute(
                    "ALTER TABLE ducklake.$fullTableName ADD COLUMN IF NOT EXISTS ${col.name} ${col.duckType} DEFAULT NULL"
                )
            }
            for (w in diff.widenedColumns) {
                stmt.execute(
                    "ALTER TABLE ducklake.$fullTableName ALTER COLUMN ${w.name} TYPE ${w.newDuckType}"
                )
            }
            // Removed columns: no DDL — new Parquet files simply omit them
        }
    }

    private fun currentColumns(): Map<String, String> {
        return try {
            val result = mutableMapOf<String, String>()
            conn.createStatement().use { stmt ->
                stmt.executeQuery("DESCRIBE ducklake.$fullTableName").use { rs ->
                    while (rs.next()) {
                        result[rs.getString("column_name")] = rs.getString("column_type")
                    }
                }
            }
            result
        } catch (e: Exception) {
            emptyMap()
        }
    }

    private fun jdbcToDuckLakeUrl(jdbcUrl: String): String {
        // jdbc:postgresql://host:port/db?params → postgres://host:port/db?params
        return jdbcUrl.removePrefix("jdbc:")
            .replace("postgresql://", "postgres://")
    }

    override fun close() {
        conn.close()
    }
}
