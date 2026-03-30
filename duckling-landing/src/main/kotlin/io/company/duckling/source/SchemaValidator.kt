package io.company.duckling.source

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema

/**
 * Fetches and validates Avro schemas from Apicurio Registry.
 * Enforces BACKWARD compatibility mode and flat type rules.
 */
class SchemaValidator(private val registryUrl: String) {

    private val http = java.net.http.HttpClient.newHttpClient()
    private val json = ObjectMapper()

    // DuckDB reserved keywords (subset — most commonly conflicting ones)
    private val duckdbReserved = setOf(
        "select", "from", "where", "table", "column", "index", "order", "group",
        "by", "having", "limit", "offset", "join", "inner", "outer", "left",
        "right", "full", "cross", "on", "as", "and", "or", "not", "null",
        "true", "false", "in", "is", "like", "between", "case", "when", "then",
        "else", "end", "insert", "update", "delete", "create", "drop", "alter",
        "with", "union", "intersect", "except", "all", "distinct", "top",
        "primary", "key", "foreign", "references", "unique", "check", "default",
        "constraint", "view", "database", "schema", "use", "show", "describe"
    )

    data class ValidatedSchema(
        val avroSchema: Schema,
        val columns: List<ColumnDef>
    )

    data class ColumnDef(
        val name: String,
        val duckType: String,
        val nullable: Boolean
    )

    fun fetchAndValidate(groupId: String, artifactId: String): ValidatedSchema {
        val compatMode = fetchCompatibilityMode(groupId, artifactId)
        check(compatMode == "BACKWARD") {
            "Schema $groupId/$artifactId has compatibility mode '$compatMode', expected BACKWARD"
        }

        val schemaJson = fetchLatestSchema(groupId, artifactId)
        val schema = Schema.Parser().parse(schemaJson)

        return ValidatedSchema(schema, extractColumns(schema))
    }

    fun extractColumns(schema: Schema): List<ColumnDef> {
        require(schema.type == Schema.Type.RECORD) { "Top-level schema must be RECORD, got ${schema.type}" }

        return schema.fields.map { field ->
            val name = field.name()
            validateFieldName(name)
            val (duckType, nullable) = mapAvroType(field.schema(), field.name())
            ColumnDef(name, duckType, nullable)
        }
    }

    private fun validateFieldName(name: String) {
        require(name.matches(Regex("[a-zA-Z0-9_]+"))) {
            "Field name '$name' contains characters outside [a-zA-Z0-9_]"
        }
        require(name.lowercase() !in duckdbReserved) {
            "Field name '$name' is a DuckDB reserved keyword"
        }
    }

    private fun mapAvroType(schema: Schema, fieldName: String): Pair<String, Boolean> {
        return when (schema.type) {
            Schema.Type.STRING  -> Pair("VARCHAR", false)
            Schema.Type.INT     -> Pair("BIGINT", false)
            Schema.Type.LONG    -> Pair("BIGINT", false)
            Schema.Type.FLOAT   -> Pair("DOUBLE", false)
            Schema.Type.DOUBLE  -> Pair("DOUBLE", false)
            Schema.Type.BOOLEAN -> Pair("BOOLEAN", false)
            Schema.Type.NULL    -> Pair("VARCHAR", true) // null-only field → nullable VARCHAR
            Schema.Type.UNION   -> mapUnionType(schema, fieldName)
            Schema.Type.RECORD, Schema.Type.ARRAY, Schema.Type.MAP ->
                error("Field '$fieldName' has forbidden type ${schema.type} (nested objects/arrays/maps not allowed)")
            else -> error("Field '$fieldName' has unsupported Avro type ${schema.type}")
        }
    }

    private fun mapUnionType(schema: Schema, fieldName: String): Pair<String, Boolean> {
        val types = schema.types
        val nonNull = types.filter { it.type != Schema.Type.NULL }

        require(nonNull.size == 1) {
            "Field '$fieldName' has union with multiple non-null types: $nonNull"
        }
        require(types.any { it.type == Schema.Type.NULL }) {
            "Field '$fieldName' has union without null — only union[null, T] is supported"
        }

        val (duckType, _) = mapAvroType(nonNull[0], fieldName)
        return Pair(duckType, true)
    }

    private fun fetchCompatibilityMode(groupId: String, artifactId: String): String {
        val url = "$registryUrl/groups/$groupId/artifacts/$artifactId/rules/COMPATIBILITY"
        val request = java.net.http.HttpRequest.newBuilder()
            .uri(java.net.URI.create(url))
            .GET()
            .build()

        val response = http.send(request, java.net.http.HttpResponse.BodyHandlers.ofString())

        // 404 means no rule set — fall back to global rule check
        if (response.statusCode() == 404) {
            return fetchGlobalCompatibilityMode()
        }

        check(response.statusCode() == 200) {
            "Failed to fetch compatibility rule for $groupId/$artifactId: HTTP ${response.statusCode()}"
        }

        val node = json.readTree(response.body())
        return node.get("config")?.asText() ?: "NONE"
    }

    private fun fetchGlobalCompatibilityMode(): String {
        val url = "$registryUrl/admin/rules/COMPATIBILITY"
        val request = java.net.http.HttpRequest.newBuilder()
            .uri(java.net.URI.create(url))
            .GET()
            .build()

        val response = http.send(request, java.net.http.HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() == 404) return "NONE"

        val node = json.readTree(response.body())
        return node.get("config")?.asText() ?: "NONE"
    }

    private fun fetchLatestSchema(groupId: String, artifactId: String): String {
        val url = "$registryUrl/groups/$groupId/artifacts/$artifactId"
        val request = java.net.http.HttpRequest.newBuilder()
            .uri(java.net.URI.create(url))
            .GET()
            .build()

        val response = http.send(request, java.net.http.HttpResponse.BodyHandlers.ofString())
        check(response.statusCode() == 200) {
            "Failed to fetch schema for $groupId/$artifactId: HTTP ${response.statusCode()}"
        }

        return response.body()
    }
}
