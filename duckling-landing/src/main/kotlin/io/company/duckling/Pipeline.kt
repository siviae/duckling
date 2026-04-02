package io.company.duckling

import io.company.duckling.config.TopicConfig
import io.company.duckling.sink.DuckLakeWriter
import io.company.duckling.source.KafkaSource
import io.company.duckling.source.SchemaValidator
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag.of
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

private val log = LoggerFactory.getLogger("Pipeline")

suspend fun runTopicPipeline(
    cfg: TopicConfig,
    appBrokers: String,
    registryUrl: String,
    catalogUrl: String,
    s3Endpoint: String,
    s3AccessKey: String,
    s3SecretKey: String,
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

    val initialSchema = try {
        validator.fetchAndValidate(cfg.isName, cfg.name)
    } catch (e: Exception) {
        stopTopic(e.message ?: "schema_validation_failed")
        return
    }

    val source = KafkaSource(appBrokers, registryUrl, cfg.group_id, cfg.name)
    //TODO since ensureTable will be called on init, this can be used using with (because it is autocloseable)
    val writer = DuckLakeWriter(catalogUrl, cfg.isName, cfg.tableName, s3Endpoint, s3AccessKey, s3SecretKey)

    try {
        writer.ensureTable(initialSchema.columns)
    } catch (e: Exception) {
        stopTopic("table_init_failed: ${e.message}")
        source.close()
        writer.close()
        return
    }

    val recordChannel = Channel<ConsumerRecord<ByteArray, GenericRecord>>(capacity = 1000)

    try {
        coroutineScope {
            val producer = launch(Dispatchers.IO) {
                try {
                    while (isActive) {
                        val polled = source.poll(Duration.ofMillis(500))
                        for (record in polled) recordChannel.send(record)
                    }
                } finally {
                    recordChannel.close()
                }
            }

            var currentSchema = initialSchema
            val batch = mutableListOf<GenericRecord>()
            val batchStartOffsets = mutableMapOf<Int, Long>()
            var batchBytes = 0L
            var lastFlushTime = Instant.now()
            var lastMetaPoll = Instant.now()

            while (isActive) {
                val now = Instant.now()

                val record = recordChannel.tryReceive().getOrNull()
                if (record != null) {
                    batchStartOffsets.putIfAbsent(record.partition(), record.offset())
                    batch.add(record.value())
                    batchBytes += estimateBytes(record.value())
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
                        val watermarks = withContext(Dispatchers.IO) { source.highWatermarks() }
                        val committedOffs = withContext(Dispatchers.IO) { source.currentOffsets() }
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

                    if (refreshed.columns != currentSchema.columns) {
                        try {
                            writer.ensureTable(refreshed.columns)
                        } catch (e: Exception) {
                            stopTopic("schema_evolution_failed: ${e.message}")
                            producer.cancel()
                            return@coroutineScope
                        }
                        currentSchema = refreshed
                    }

                    val flushStart = Instant.now()
                    try {
                        withContext(Dispatchers.IO) {
                            writer.writeBatch(batch, currentSchema.columns)
                            source.commitSync()
                        }
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
    }
}

/** Estimates heap bytes consumed by one GenericRecord. Not exact — used only for budget tracking. */
private fun estimateBytes(record: GenericRecord): Long =
    record.schema.fields.sumOf { field ->
        when (val v = record.get(field.name())) {
            is String -> v.length.toLong() * 2 + 24  // UTF-16 chars + object header
            is ByteArray -> v.size.toLong()
            else -> 8L                           // Long, Double, Boolean, null
        }
    }
