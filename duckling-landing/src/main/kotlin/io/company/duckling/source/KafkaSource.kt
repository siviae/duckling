package io.company.duckling.source

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

data class KafkaRecord(
    val partition: Int,
    val offset: Long,
    val value: Map<String, Any?>,
)

class KafkaSource(
    brokers: String,
    groupId: String,
    private val topic: String,
) : AutoCloseable {

    private val consumer: KafkaConsumer<ByteArray, String>
    private val json = ObjectMapper()

    init {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500")
        }
        consumer = KafkaConsumer(props)
        consumer.subscribe(listOf(topic))
    }

    @Suppress("UNCHECKED_CAST")
    fun poll(timeout: Duration): List<KafkaRecord> =
        consumer.poll(timeout).records(topic).map { r ->
            KafkaRecord(
                partition = r.partition(),
                offset = r.offset(),
                value = json.readValue(r.value(), Map::class.java) as Map<String, Any?>,
            )
        }

    //TODO combine two calls below so consumer.partitionsFor would not be called twice
    /** Returns map of partition → high watermark (end offset). */
    fun highWatermarks(): Map<Int, Long> {
        val partitions = consumer.partitionsFor(topic).map { TopicPartition(topic, it.partition()) }
        return consumer.endOffsets(partitions).mapKeys { it.key.partition() }
    }

    /** Returns map of partition → current committed offset. */
    fun currentOffsets(): Map<Int, Long> {
        val partitions = consumer.partitionsFor(topic).map { TopicPartition(topic, it.partition()) }
        return consumer.committed(partitions.toSet())
            .mapKeys { it.key.partition() }
            .mapValues { it.value?.offset() ?: 0L }
    }

    fun commitSync() {
        consumer.commitSync()
    }

    override fun close() {
        consumer.close()
    }
}
