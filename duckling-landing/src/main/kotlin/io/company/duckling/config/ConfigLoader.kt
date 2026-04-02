package io.company.duckling.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.io.File

object ConfigLoader {

    private val mapper = ObjectMapper(YAMLFactory()).registerKotlinModule()

    fun load(path: String): AppConfig {
        val raw = File(path).readText()
        return mapper.readValue(raw, DucklingConfig::class.java).duckling
    }
}
