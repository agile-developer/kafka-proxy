package io.conduktor.assignment.service

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties

class OnDemandConsumer {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun consume(bootstrapServer: String, topic: String): List<String> {
        val properties = Properties()
        properties["bootstrap.servers"] = bootstrapServer
        properties["enable.auto.commit"] = false
        properties["auto.offset.reset"] = "earliest"
        properties["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        properties["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        // No 'group.id' since we'll use manual partition assignment

        val kafkaConsumer = KafkaConsumer<String, String>(properties)
        val partitions = kafkaConsumer.partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
        // Manually assign partitions from our topic to this consumer instance
        kafkaConsumer.assign(partitions)
        // Calling 'use()' so that 'kafkaConsumer.close()' gets called automatically
        return kafkaConsumer.use { kc ->
            val consumed = kc.poll(Duration.ofSeconds(1L))
            val recordValues = consumed.map { it.value() }.toList()
            // We may or may not want to commit here
            // kc.commitSync()
            recordValues
        }
    }
}
