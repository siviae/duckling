package io.company.duckling.sink

import io.company.duckling.source.SchemaValidator.ColumnDef

/**
 * Computes the DDL changes needed to evolve a DuckDB table from its current
 * column set to match a new Avro schema.
 */
object SchemaEvolution {

    data class Diff(
        val addedColumns: List<ColumnDef>,
        val widenedColumns: List<WideningDef>,
        val removedColumnNames: List<String>
    )

    data class WideningDef(
        val name: String,
        val newDuckType: String
    )

    /**
     * @param currentColumns  columns currently in DuckDB (name → duckType)
     * @param newSchema       columns from the latest Apicurio schema
     */
    fun compute(currentColumns: Map<String, String>, newSchema: List<ColumnDef>): Diff {
        val newByName = newSchema.associateBy { it.name }

        val added = newSchema.filter { it.name !in currentColumns }

        val widened = newSchema.mapNotNull { col ->
            val current = currentColumns[col.name] ?: return@mapNotNull null
            when {
                current == col.duckType -> null
                current == "BIGINT" && col.duckType == "DOUBLE" -> WideningDef(col.name, "DOUBLE")
                else -> error(
                    "Non-widening type change for column '${col.name}': $current → ${col.duckType}. " +
                    "Only BIGINT → DOUBLE is permitted."
                )
            }
        }

        val removed = currentColumns.keys.filter { it !in newByName }

        return Diff(added, widened, removed)
    }
}
