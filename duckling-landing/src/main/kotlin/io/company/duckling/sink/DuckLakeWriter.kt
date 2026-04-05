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
    isName: String,
    tableName: String,
    s3Endpoint: String = "",
    s3AccessKey: String = "",
    s3SecretKey: String = "",
    s3Region: String = "",
) : AutoCloseable {

    private val conn: Connection
    private val fullTableName = "${isName}__${tableName.replace(".", "__")}"
    private val s3Path = "s3://$isName/ducklake/"

    init {
        Class.forName("org.duckdb.DuckDBDriver")
        conn = DriverManager.getConnection("jdbc:duckdb:")

        conn.createStatement().use { stmt ->
            stmt.execute("INSTALL ducklake")
            stmt.execute("LOAD ducklake")

            if (s3Endpoint.isNotBlank() && s3AccessKey.isNotBlank()) {
                val endpoint = s3Endpoint.removePrefix("http://").removePrefix("https://")
                val useSsl = s3Endpoint.startsWith("https://")
                val regionClause = if (s3Region.isNotBlank()) ",\n                    REGION '$s3Region'" else ""
                stmt.execute("""
                    CREATE SECRET duckling_s3 (
                        TYPE S3,
                        KEY_ID '$s3AccessKey',
                        SECRET '$s3SecretKey',
                        ENDPOINT '$endpoint',
                        USE_SSL $useSsl,
                        URL_STYLE 'path'$regionClause
                    )
                """.trimIndent())
            }

            val duckLakeUrl = jdbcToDuckLakeUrl(catalogJdbcUrl)
            stmt.execute(
                """
                ATTACH '$duckLakeUrl' AS ducklake (DATA_PATH '$s3Path', OVERRIDE_DATA_PATH TRUE, DATA_INLINING_ROW_LIMIT 0)
            """.trimIndent()
            )
        }
    }

    /** Ensures the table exists with the given columns, applying evolution DDL as needed. */
    //TODO do this on init
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

        // Stage into a temp in-memory DuckDB table, then INSERT SELECT into DuckLake in one shot.
        // Direct multi-row INSERT into DuckLake via JDBC prepared statements (even with explicit
        // BEGIN/COMMIT) does not create a DuckLake data snapshot. A single INSERT...SELECT lets
        // DuckLake manage its own transaction and creates exactly one Parquet snapshot.
        val tempTable = "_batch_stage"
        val colDefs = columns.joinToString(", ") { col ->
            val nullability = if (col.nullable) "" else " NOT NULL"
            "${col.name} ${col.duckType}$nullability"
        }
        conn.createStatement().use { it.execute("CREATE OR REPLACE TEMP TABLE $tempTable ($colDefs)") }

        try {
            conn.prepareStatement("INSERT INTO $tempTable ($colNames) VALUES ($placeholders)").use { ps ->
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

            // Single auto-committed statement — DuckLake creates one snapshot for the entire batch
            conn.createStatement().use {
                it.execute("INSERT INTO ducklake.$fullTableName SELECT * FROM $tempTable")
            }
        } finally {
            try { conn.createStatement().use { it.execute("DROP TABLE IF EXISTS $tempTable") } } catch (_: Exception) {}
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
        // jdbc:postgresql://host:port/db?user=u&password=p
        // → ducklake:postgres:host=host port=port dbname=db user=u password=p
        val withoutPrefix = jdbcUrl.removePrefix("jdbc:postgresql://")
        val (hostPart, rest) = withoutPrefix.split("/", limit = 2)
        val (host, port) = if (":" in hostPart) hostPart.split(":", limit = 2) else listOf(hostPart, "5432")
        val (dbName, queryString) = if ("?" in rest) rest.split("?", limit = 2) else listOf(rest, "")
        val params = queryString.split("&")
            .filter { it.isNotEmpty() }
            .joinToString(" ") { it.replace("=", "=") }
        val pgConn = "host=$host port=$port dbname=$dbName" +
            (if (params.isNotEmpty()) " $params" else "")
        return "ducklake:postgres:$pgConn"
    }

    override fun close() {
        conn.close()
    }
}
