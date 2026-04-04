package io.company.duckling

import com.sun.net.httpserver.HttpServer
import io.company.duckling.config.ConfigLoader
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress

private val log = LoggerFactory.getLogger("Main")

fun main(args: Array<String>) {
    val configPath = parseConfigPath(args)
    log.info("Loading config from $configPath")

    val config = ConfigLoader.load(configPath)
    log.info("Loaded config: ${config.topics.size} topic(s)")

    val meterRegistry: MeterRegistry? = config.metrics.prometheus?.let { promCfg ->
        val reg = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        startPrometheusHttpServer(promCfg.port, reg)
        log.info("Prometheus metrics on :{}/metrics", promCfg.port)
        reg
    }

    val controller = AdaptiveController(
        safetyFactor = config.adaptive.safety_factor,
        emaAlpha = config.adaptive.ema_alpha,
    )
    config.topics.forEach { controller.register(it.name) }
    log.info(
        "Adaptive controller: safety_factor=${config.adaptive.safety_factor}" +
                " ema_alpha=${config.adaptive.ema_alpha}" +
                " usable_heap=${
                    (Runtime.getRuntime().maxMemory() * config.adaptive.safety_factor / (1024 * 1024)).toInt()
                } MB"
    )

    runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

        for (topic in config.topics) {
            scope.launch {
                log.info("Starting pipeline for topic '${topic.name}'")
                try {
                    runTopicPipeline(
                        cfg = topic,
                        appBrokers = config.kafka.brokers,
                        registryUrl = config.kafka.schema_registry,
                        catalogUrl = config.ducklake.catalog_url,
                        s3Endpoint = config.ducklake.s3_endpoint,
                        s3AccessKey = config.ducklake.s3_access_key,
                        s3SecretKey = config.ducklake.s3_secret_key,
                        s3Region = config.ducklake.s3_region,
                        controller = controller,
                        registry = meterRegistry,
                    )
                } catch (e: Exception) {
                    log.error("Pipeline for '${topic.name}' terminated", e)
                }
            }
        }

        awaitCancellation()
    }
}

private fun parseConfigPath(args: Array<String>): String {
    val idx = args.indexOf("--config")
    return if (idx >= 0 && idx + 1 < args.size) args[idx + 1] else "duckling.yaml"
}

private fun startPrometheusHttpServer(port: Int, registry: PrometheusMeterRegistry) {
    val server = HttpServer.create(InetSocketAddress(port), 0)
    server.createContext("/metrics") { exchange ->
        val body = registry.scrape().toByteArray()
        exchange.responseHeaders.set("Content-Type", "text/plain; version=0.0.4")
        exchange.sendResponseHeaders(200, body.size.toLong())
        exchange.responseBody.use { it.write(body) }
    }
    server.executor = null  // use default
    server.start()
}
