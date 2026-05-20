package io.company.duckling

import java.sql.DriverManager

/** Standalone entry point used only at Docker image build time to pre-install the DuckLake
 *  extension so the first `INSTALL ducklake` at runtime is a fast no-op. */
fun main() {
    Class.forName("org.duckdb.DuckDBDriver")
    DriverManager.getConnection("jdbc:duckdb:").use { conn ->
        conn.createStatement().use { stmt ->
            stmt.execute("INSTALL ducklake")
            stmt.execute("LOAD ducklake")
        }
    }
    println("DuckLake extension pre-installed.")
}
