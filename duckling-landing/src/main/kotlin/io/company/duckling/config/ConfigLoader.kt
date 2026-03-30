package io.company.duckling.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.io.File

object ConfigLoader {

    private val mapper = ObjectMapper(YAMLFactory()).registerKotlinModule()

    fun load(path: String): AppConfig {
        val raw = File(path).readText()
        val substituted = substituteEnvVars(raw)
        return mapper.readValue(substituted, DucklingConfig::class.java).duckling
    }

    private fun substituteEnvVars(text: String): String {
        val pattern = Regex("""\$\{([^}]+)}""")
        val unresolved = mutableListOf<String>()

        val result = pattern.replace(text) { match ->
            val varName = match.groupValues[1]
            System.getenv(varName) ?: run {
                unresolved.add(varName)
                match.value
            }
        }

        if (unresolved.isNotEmpty()) {
            error("Unresolved environment variables in config: ${unresolved.joinToString(", ")}")
        }

        return result
    }
}
