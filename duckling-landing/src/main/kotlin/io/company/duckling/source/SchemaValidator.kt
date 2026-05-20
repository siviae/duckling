package io.company.duckling.source

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory

/**
 * Fetches and validates JSON Schemas from Apicurio Registry.
 * Enforces BACKWARD compatibility mode and maps JSON Schema types to DuckDB types.
 *
 * Expected JSON Schema format:
 * {
 *   "$schema": "http://json-schema.org/draft-07/schema#",
 *   "type": "object",
 *   "properties": {
 *     "field_name": { "type": "string" },
 *     "amount":     { "type": "number" },
 *     "count":      { "type": "integer" },
 *     "active":     { "type": "boolean" },
 *     "nullable":   { "type": ["string", "null"] }
 *   },
 *   "required": ["field_name", "amount"]
 * }
 *
 * Type mapping: string→VARCHAR, number→DOUBLE, integer→BIGINT, boolean→BOOLEAN.
 * Fields absent from "required" are nullable. Union ["T","null"] is also nullable.
 */
class SchemaValidator(private val registryUrl: String) {

    private val log = LoggerFactory.getLogger(SchemaValidator::class.java)
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

    data class ValidatedSchema(val columns: List<ColumnDef>)

    data class ColumnDef(
        val name: String,
        val duckType: String,
        val nullable: Boolean,
    )

    fun fetchAndValidate(groupId: String, artifactId: String): ValidatedSchema {
        val compatMode = fetchCompatibilityMode(groupId, artifactId)
        check(compatMode == "BACKWARD") {
            "Schema $groupId/$artifactId has compatibility mode '$compatMode', expected BACKWARD"
        }

        val schemaJson = fetchLatestSchema(groupId, artifactId)
        val root = json.readTree(schemaJson)
        return ValidatedSchema(extractColumns(root))
    }

    fun extractColumns(root: JsonNode): List<ColumnDef> {
        val properties = root.get("properties")
            ?: error("JSON Schema missing 'properties' object")
        val required = root.get("required")
            ?.map { it.asText() }
            ?.toSet()
            ?: emptySet()

        return properties.fields().asSequence().map { (name, fieldNode) ->
            validateFieldName(name)
            mapField(name, fieldNode, name in required)
        }.toList()
    }

    private fun mapField(name: String, node: JsonNode, required: Boolean): ColumnDef {
        val typeNode = node.get("type")
            ?: error("Field '$name' has no 'type' in JSON Schema")

        return when {
            typeNode.isArray -> {
                // e.g. ["string", "null"] or ["null", "string"]
                val types = typeNode.map { it.asText() }
                val nonNull = types.filter { it != "null" }
                require(nonNull.size == 1) {
                    "Field '$name' has union with multiple non-null types: $types"
                }
                ColumnDef(name, scalarToDuck(nonNull[0], name), nullable = true)
            }
            typeNode.isTextual -> {
                ColumnDef(name, scalarToDuck(typeNode.asText(), name), nullable = !required)
            }
            else -> error("Field '$name' has unsupported type node: $typeNode")
        }
    }

    private fun scalarToDuck(type: String, fieldName: String): String = when (type) {
        "string"  -> "VARCHAR"
        "number"  -> "DOUBLE"
        "integer" -> "BIGINT"
        "boolean" -> "BOOLEAN"
        // Nested types: store as JSON strings — no lossless DuckDB column type equivalent
        "object", "array" -> {
            log.warn("Field '$fieldName' has complex type '$type', mapping to VARCHAR")
            "VARCHAR"
        }
        else -> error("Field '$fieldName' has unsupported JSON Schema type '$type'")
    }

    private fun validateFieldName(name: String) {
        require(name.matches(Regex("[a-zA-Z0-9_]+"))) {
            "Field name '$name' contains characters outside [a-zA-Z0-9_]"
        }
        require(name.lowercase() !in duckdbReserved) {
            "Field name '$name' is a DuckDB reserved keyword"
        }
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
