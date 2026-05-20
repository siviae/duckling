package io.company.duckling

import io.company.duckling.config.TopicConfig
import io.company.duckling.sink.DuckLakeWriter
import io.company.duckling.source.KafkaRecord
import io.company.duckling.source.KafkaSource
import io.company.duckling.source.SchemaValidator
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag.of
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executors

private val log = LoggerFactory.getLogger("Pipeline")

suspend fun runTopicPipeline(
    cfg: TopicConfig,
    appBrokers: String,
    registryUrl: String,
    catalogUrl: String,
    s3Endpoint: String,
    s3AccessKey: String,
    s3SecretKey: String,
    s3Region: String,
    controller: AdaptiveController,
    registry: MeterRegistry?,
) {
    val recordsConsumed = registry?.counter("duckling_records_consumed_total", "topic", cfg.name)
    val flushesTotal = registry?.counter("duckling_flushes_total", "topic", cfg.name)
    val batchBytesHist = registry?.summary("duckling_batch_flushed_bytes", "topic", cfg.name)

    // Adaptive gauges — re-read from controller each scrape
    var currentBudget = controller.budgetFor(cfg.name, cfg)
    registry?.gauge(
        "duckling_adaptive_max_batch_bytes",
        listOf(of("topic", cfg.name)),
        currentBudget
    ) { it.maxBatchBytes.toDouble() }
    registry?.gauge(
        "duckling_adaptive_max_interval_seconds",
        listOf(of("topic", cfg.name)),
        currentBudget
    ) { it.maxFlushIntervalSeconds.toDouble() }

    fun stopTopic(reason: String) {
        log.error("Stopping topic '${cfg.name}': $reason")
        registry?.counter("duckling_topic_stopped_total", "topic", cfg.name, "reason", reason)
            ?.increment()
        controller.deregister(cfg.name)
    }

    val validator = SchemaValidator(registryUrl)

    // Retry until the schema is available (Apicurio may not be ready at startup).
    val initialSchema = run {
        while (true) {
            try {
                return@run validator.fetchAndValidate(cfg.isName, cfg.name)
            } catch (e: Exception) {
                log.warn("Schema not ready for '${cfg.name}': ${e.message}. Retrying in 2s…")
                delay(2_000)
            }
        }
        @Suppress("UNREACHABLE_CODE") error("unreachable")
    }

    val source = KafkaSource(appBrokers, cfg.group_id, cfg.name)
    //TODO since ensureTable will be called on init, this can be used using with (because it is autocloseable)
    val writer = DuckLakeWriter(catalogUrl, cfg.isName, cfg.tableName, s3Endpoint, s3AccessKey, s3SecretKey, s3Region)

    try {
        writer.ensureTable(initialSchema.columns)
    } catch (e: Exception) {
        stopTopic("table_init_failed: ${e.message}")
        source.close()
        writer.close()
        return
    }

    val recordChannel = Channel<KafkaRecord>(capacity = 1000)

    // Single-threaded context for all Kafka operations — KafkaConsumer is not thread-safe
    val kafkaExecutor = Executors.newSingleThreadExecutor { r -> Thread(r, "kafka-${cfg.name}") }
    val kafkaContext = kafkaExecutor.asCoroutineDispatcher()

    try {
        coroutineScope {
            val producer = launch(kafkaContext) {
                try {
                    while (isActive) {
                        val polled = source.poll(Duration.ofMillis(500))
                        for (record in polled) recordChannel.send(record)
                        // Yield so that withContext(kafkaContext) calls from the consumer loop
                        // (highWatermarks, currentOffsets, commitSync) can run on this thread.
                        yield()
                    }
                } finally {
                    recordChannel.close()
                }
            }

            var currentSchema = initialSchema
            val batch = mutableListOf<Map<String, Any?>>()
            val batchStartOffsets = mutableMapOf<Int, Long>()
            var batchBytes = 0L
            var lastFlushTime = Instant.now()
            var lastMetaPoll = Instant.now()

            while (isActive) {
                val now = Instant.now()

                val record = recordChannel.tryReceive().getOrNull()
                if (record != null) {
                    // Reset flush timer when a new batch starts (not at pipeline startup)
                    // so the 30s window measures time-since-first-record, not time-since-init
                    if (batch.isEmpty()) lastFlushTime = Instant.now()
                    batchStartOffsets.putIfAbsent(record.partition, record.offset)
                    batch.add(record.value)
                    batchBytes += estimateBytes(record.value)
                    recordsConsumed?.increment()
                } else if (recordChannel.isClosedForReceive) {
                    break
                }

                // Offset-progress check
                val metaElapsed = Duration.between(lastMetaPoll, now).toMillis()
                var flushByOffset = false
                if (metaElapsed >= cfg.metadata_poll_interval_seconds * 1000L && batch.isNotEmpty()) {
                    lastMetaPoll = now
                    try {
                        val watermarks = withContext(kafkaContext) { source.highWatermarks() }
                        val committedOffs = withContext(kafkaContext) { source.currentOffsets() }
                        for ((partition, startOffset) in batchStartOffsets) {
                            val hwm = watermarks[partition] ?: continue
                            val current = committedOffs[partition] ?: startOffset
                            val lag = hwm - startOffset
                            if (lag > 0 && (current - startOffset).toDouble() / lag > cfg.offset_progress_flush_threshold) {
                                flushByOffset = true
                                break
                            }
                        }
                    } catch (e: Exception) {
                        log.warn("Watermark poll failed for '${cfg.name}': ${e.message}")
                    }
                }

                val elapsed = Duration.between(lastFlushTime, now).toMillis()
                val pastMin = elapsed >= cfg.min_flush_interval_seconds * 1000L
                val pastEffectiveMax = elapsed >= currentBudget.maxFlushIntervalSeconds * 1000L
                val overBudget = batchBytes >= currentBudget.maxBatchBytes

                // Byte trigger bypasses pastMin — it is a safety valve.
                // Time and offset triggers respect the minimum interval.
                val shouldFlush = batch.isNotEmpty() &&
                        (overBudget || (pastMin && (pastEffectiveMax || flushByOffset)))

                if (shouldFlush) {
                    val refreshed = try {
                        withContext(Dispatchers.IO) { validator.fetchAndValidate(cfg.isName, cfg.name) }
                    } catch (e: Exception) {
                        stopTopic(e.message ?: "schema_refresh_failed")
                        producer.cancel()
                        return@coroutineScope
                    }

                    // Always ensureTable — handles schema changes AND recreates dropped tables
                    try {
                        withContext(Dispatchers.IO) { writer.ensureTable(refreshed.columns) }
                    } catch (e: Exception) {
                        stopTopic("schema_evolution_failed: ${e.message}")
                        producer.cancel()
                        return@coroutineScope
                    }
                    currentSchema = refreshed

                    val flushStart = Instant.now()
                    try {
                        withContext(Dispatchers.IO) { writer.writeBatch(batch, currentSchema.columns) }
                        withContext(kafkaContext) { source.commitSync() }
                    } catch (e: Exception) {
                        stopTopic("flush_failed: ${e.message}")
                        producer.cancel()
                        return@coroutineScope
                    }

                    val elapsedSeconds = Duration.between(lastFlushTime, flushStart).toMillis() / 1000.0
                    controller.recordFlush(cfg.name, batchBytes, elapsedSeconds)
                    currentBudget = controller.budgetFor(cfg.name, cfg)

                    flushesTotal?.increment()
                    batchBytesHist?.record(batchBytes.toDouble())
                    log.info(
                        "Flushed ${batch.size} records (${batchBytes / 1024} KB) for '${cfg.name}'" +
                                " — next budget: ${currentBudget.maxBatchBytes / (1024 * 1024)} MB" +
                                " / ${currentBudget.maxFlushIntervalSeconds}s"
                    )

                    batch.clear()
                    batchStartOffsets.clear()
                    batchBytes = 0L
                    lastFlushTime = Instant.now()
                }

                if (record == null) delay(50)
            }

            producer.cancel()
        }
    } finally {
        source.close()
        writer.close()
        kafkaContext.close()
    }
}

/** Estimates heap bytes consumed by one JSON record. Not exact — used only for budget tracking. */
private fun estimateBytes(record: Map<String, Any?>): Long =
    record.values.sumOf { v ->
        when (v) {
            is String -> v.length.toLong() * 2 + 24  // UTF-16 chars + object header
            is ByteArray -> v.size.toLong()
            else -> 8L                                // Number, Boolean, null
        }
    }
