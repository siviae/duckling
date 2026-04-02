package io.company.duckling.source

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.time.Duration
import java.util.Properties

class KafkaSource(
    brokers: String,
    registryUrl: String,
    groupId: String,
    private val topic: String
) : AutoCloseable {

    private val consumer: KafkaConsumer<ByteArray, GenericRecord>

    init {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer::class.java.name)
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500")
            // Apicurio registry URL
            put("apicurio.registry.url", registryUrl)
            put("apicurio.registry.avro-datum-provider", "io.apicurio.registry.serde.avro.ReflectAvroDatumProvider")
        }
        consumer = KafkaConsumer(props)
        consumer.subscribe(listOf(topic))
    }

    fun poll(timeout: Duration): Iterable<ConsumerRecord<ByteArray, GenericRecord>> {
        val records = consumer.poll(timeout)
        return records.records(topic)
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
