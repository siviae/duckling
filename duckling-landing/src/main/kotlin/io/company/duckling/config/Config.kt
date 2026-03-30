package io.company.duckling.config

data class DucklingConfig(
    val duckling: AppConfig
)

data class AppConfig(
    val kafka: KafkaConfig,
    val ducklake: DuckLakeConfig,
    val metrics: MetricsConfig = MetricsConfig(),
    val adaptive: AdaptiveConfig = AdaptiveConfig(),
    val topics: List<TopicConfig>
)

data class KafkaConfig(
    val brokers: String,
    val schema_registry: String
)

data class DuckLakeConfig(
    val catalog_url: String
)

data class MetricsConfig(
    val prometheus: PrometheusConfig? = null
)

data class PrometheusConfig(
    val port: Int = 9090
)

data class AdaptiveConfig(
    val safety_factor: Double = 0.65,
    val ema_alpha: Double = 0.3,
)

data class TopicConfig(
    val name: String,
    val group_id: String,
    val table: String,
    val min_flush_interval_seconds: Int = 30,
    val max_flush_interval_seconds: Int = 300,
    val metadata_poll_interval_seconds: Int = 10,
    val offset_progress_flush_threshold: Double = 0.5,
    val max_batch_bytes: Long = 64 * 1024 * 1024,
) {
    val isName: String get() = table.substringBefore(".")
    val tableName: String get() = table.substringAfter(".")
}
