package io.conduktor.assignment.service

import io.conduktor.assignment.domain.PartitionInfo
import io.conduktor.assignment.domain.TopicInfo
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class TopicService(
    private val clusterConnectionProvider: ClusterConnectionProvider
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun listForCluster(bootstrapServer: String): TopicResult {
        logger.info("Listing topics for cluster: $bootstrapServer")
        val adminClient = getAdminClient(bootstrapServer)
            ?: return TopicResult.Error("Cluster: $bootstrapServer does not have an active connection")
        val topicNames = adminClient.listTopics(ListTopicsOptions().listInternal(false)).names().get()
        if (topicNames.isEmpty()) {
            return TopicResult.NotFound(bootstrapServer)
        }
        logger.info("Found ${topicNames.size} topics for cluster: $bootstrapServer")
        val topicNamesToTopicDescriptions = adminClient.describeTopics(topicNames).allTopicNames().get()
        val topicInfos = topicNamesToTopicDescriptions.map { topicEntry ->
            TopicInfo(
                topicEntry.key,
                topicEntry.value.partitions().map { partitionInfo ->
                    PartitionInfo(partitionInfo.partition()) }.toSet()
            )
        }
        return TopicResult.Found(topicInfos)
    }

    fun isValidTopic(bootstrapServer: String, topic: String): Boolean {
        val adminClient = getAdminClient(bootstrapServer) ?: return false
        val isValid = adminClient.listTopics().names().get().contains(topic)
        if (!isValid) {
            logger.info("Cluster: $bootstrapServer does not have a topic named: $topic")
        }
        return isValid
    }

    fun createTopic(bootstrapServer: String, topic: String): CreateTopicResult {
        val connectionPair = clusterConnectionProvider.getConnection(bootstrapServer) ?:
            return CreateTopicResult.Error("Cluster: $bootstrapServer does not have an active connection")
        val admin = connectionPair.second
        val newTopic = NewTopic(topic, 1, 1)
        val createTopicsResult = admin.createTopics(mutableListOf(newTopic))
        val topicId = createTopicsResult.topicId(topic).get()
        logger.info("Created topic: $topic with id: $topicId for cluster: $bootstrapServer")
        return CreateTopicResult.Success(topicId.toString())
    }

    fun consumeRecords(bootstrapServer: String, topic: String): ConsumeResult {
        logger.info("Consuming records from cluster: $bootstrapServer, topic: $topic")
        try {
            val consumedRecords = OnDemandConsumer().consume(bootstrapServer = bootstrapServer, topic = topic)
            if (consumedRecords.isEmpty()) {
                logger.info("Consuming from cluster: $bootstrapServer, topic: $topic returned no records, returning empty list")
            }
            return ConsumeResult.HasRecords(consumedRecords)
        } catch (e: Exception) {
            logger.error("Exception encountered while consuming records for cluster: $bootstrapServer and topic: $topic", e)
            return ConsumeResult.Error("Exception encountered while consuming records for cluster: $bootstrapServer and topic: $topic")
        }
    }

    private fun getAdminClient(bootstrapServer: String): Admin? {
        return clusterConnectionProvider.getConnection(bootstrapServer)?.second
    }
}

sealed interface TopicResult {
    data class Found(val topics: List<TopicInfo>): TopicResult
    data class NotFound(val bootstrapServer: String): TopicResult {
        val message get() = "No topics found for cluster: $bootstrapServer"
    }
    data class Error(val message: String): TopicResult
}

sealed interface ConsumeResult {
    data class HasRecords(val records: List<String>): ConsumeResult
    data class Error(val message: String): ConsumeResult
}

sealed interface CreateTopicResult {
    data class Success(val id: String): CreateTopicResult
    data class Error(val message: String): CreateTopicResult
}
