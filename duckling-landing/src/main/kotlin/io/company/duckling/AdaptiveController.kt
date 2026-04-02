package io.company.duckling

import io.company.duckling.config.TopicConfig
import java.util.concurrent.ConcurrentHashMap

/**
 * Shared controller that computes per-topic flush budgets from the JVM heap
 * and observed throughput. One instance is created at startup and passed to
 * every topic pipeline.
 *
 * Thread-safe: pipelines run as coroutines on different threads and call
 * recordFlush / budgetFor concurrently.
 */
class AdaptiveController(
    private val safetyFactor: Double = 0.65,
    private val emaAlpha: Double = 0.3,
) {
    data class Budget(
        val maxBatchBytes: Long,
        val maxFlushIntervalSeconds: Int,
    )

    private val estimators = ConcurrentHashMap<String, EmaEstimator>()

    fun register(topic: String) {
        estimators.putIfAbsent(topic, EmaEstimator(emaAlpha))
    }

    fun deregister(topic: String) {
        estimators.remove(topic)
    }

    /** Called after each successful flush to update the throughput estimate. */
    fun recordFlush(topic: String, bytes: Long, elapsedSeconds: Double) {
        estimators[topic]?.update(bytes, elapsedSeconds)
    }

    /**
     * Returns the effective (possibly tightened) limits for the next batch cycle.
     * Configured values in [cfg] are treated as upper bounds.
     */
    fun budgetFor(topic: String, cfg: TopicConfig): Budget {
        val usableHeap = (Runtime.getRuntime().maxMemory() * safetyFactor).toLong()
        val activeTopics = estimators.size.coerceAtLeast(1)
        val heapBudget = usableHeap / activeTopics

        val maxBytes = minOf(heapBudget, cfg.max_batch_bytes)

        val bps = estimators[topic]?.bytesPerSec ?: 0.0
        val fillSeconds = if (bps > 0.0) (maxBytes / bps).toInt()
        else cfg.min_flush_interval_seconds

        val interval = fillSeconds
            .coerceIn(cfg.min_flush_interval_seconds, cfg.max_flush_interval_seconds)

        return Budget(maxBytes, interval)
    }
}

/** Exponential moving average estimator for bytes-per-second throughput. */
class EmaEstimator(private val alpha: Double = 0.3) {
    var bytesPerSec: Double = 0.0
        private set

    fun update(bytes: Long, elapsedSeconds: Double) {
        if (elapsedSeconds <= 0.0) return
        val sample = bytes.toDouble() / elapsedSeconds
        bytesPerSec = if (bytesPerSec == 0.0) sample
        else alpha * sample + (1.0 - alpha) * bytesPerSec
    }
}
