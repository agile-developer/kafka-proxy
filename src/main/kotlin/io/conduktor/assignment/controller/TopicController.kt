package io.conduktor.assignment.controller

import io.conduktor.assignment.controller.TopicInfoResponse.Companion.fromTopicInfo
import io.conduktor.assignment.domain.TopicInfo
import io.conduktor.assignment.service.ConsumeResult
import io.conduktor.assignment.service.CreateTopicResult
import io.conduktor.assignment.service.TopicResult.Error
import io.conduktor.assignment.service.TopicResult.Found
import io.conduktor.assignment.service.TopicResult.NotFound
import io.conduktor.assignment.service.TopicService
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus.NOT_FOUND
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.net.URI

@RestController
@RequestMapping("/kafka-proxy/topics")
class TopicController(
    private val topicService: TopicService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @GetMapping
    fun listTopics(
        @RequestParam
        bootstrapServer: String
    ): ResponseEntity<*> {
        validateURL(bootstrapServer)?.let { errorMessage ->
            return ResponseEntity.badRequest().body(errorMessage)
        }
        return when (val result = topicService.listForCluster(bootstrapServer)) {
            is Found -> ResponseEntity.ok(result.topics.map { fromTopicInfo(it) })
            is NotFound -> ResponseEntity.status(NOT_FOUND).body(result.message)
            is Error -> ResponseEntity.badRequest().body(result.message)
        }
    }

    @GetMapping("/records")
    fun consumeTopic(
        @RequestParam
        bootstrapServer: String,
        @RequestParam
        topic: String
    ): ResponseEntity<*> {
        if (topic.isBlank()) {
            logger.error("Topic name is blank")
            return ResponseEntity.badRequest().body("Topic name is blank")
        }
        validateURL(bootstrapServer)?.let { errorMessage ->
            return ResponseEntity.badRequest().body(errorMessage)
        }
        val isValid = topicService.isValidTopic(bootstrapServer = bootstrapServer, topic =topic)
        if (!isValid) {
            logger.error("Cluster: $bootstrapServer does not have a topic named: $topic")
            ResponseEntity.badRequest().body("Cluster: $bootstrapServer does not have a topic named: $topic")
        }
        return when (val result = topicService.consumeRecords(bootstrapServer = bootstrapServer, topic =topic)) {
            is ConsumeResult.HasRecords -> ResponseEntity.ok(result.records)
            is ConsumeResult.Error -> ResponseEntity.badRequest().body(result.message)
        }
    }

    @PostMapping
    fun createTopic(
        @RequestBody
        createTopicRequest: CreateTopicRequest
    ): ResponseEntity<*> {
        runCatching {
            createTopicRequest.validate()
        }.onFailure {
            return ResponseEntity.badRequest().body(it.message)
        }
        val bootstrapServer = createTopicRequest.bootstrapServer
        val topic = createTopicRequest.topic
        val alreadyExists = topicService.isValidTopic(bootstrapServer, topic)
        if (alreadyExists) {
            logger.info("Topic $topic already exists for cluster: $bootstrapServer")
            return ResponseEntity.ok("Topic $topic already exists for cluster: $bootstrapServer")
        }
        return when(val result = topicService.createTopic(bootstrapServer = bootstrapServer, topic = topic)) {
            is CreateTopicResult.Success -> ResponseEntity.ok(CreateTopicResponse(id = result.id, topic = topic, bootstrapServer= bootstrapServer))
            is CreateTopicResult.Error -> ResponseEntity.badRequest().body(result.message)
        }
    }

    private fun validateURL(bootstrapServer: String): String? {
        if (bootstrapServer.isBlank()) {
            logger.error("Bootstrap-server URL: $bootstrapServer is blank")
            return "Bootstrap-server URL: $bootstrapServer is blank"
        }
        try {
            URI(bootstrapServer)
        } catch (e: Exception) {
            logger.error("Bootstrap-server: $bootstrapServer is not a valid URL", e)
            return "Bootstrap-server: $bootstrapServer is not a valid URL"
        }
        return null;
    }
}

data class CreateTopicRequest(
    val bootstrapServer: String,
    val topic: String
) {
    fun validate() {
        val messages = mutableListOf<String>()
        if (bootstrapServer.isBlank()) {
            messages.add("Bootstrap-server URL: $bootstrapServer is blank")
        }
        try {
            URI(bootstrapServer)
        } catch (e: Exception) {
            messages.add("Bootstrap-server: $bootstrapServer is not a valid URL")
        }
        if (topic.isBlank()) {
            messages.add("Topic name is blank")
        }
        if (messages.isNotEmpty()) throw IllegalArgumentException(messages.joinToString("\n"))
    }
}

data class CreateTopicResponse(
    val id: String,
    val topic: String,
    val bootstrapServer: String
)

data class TopicInfoResponse(
    val name: String,
    val partitions: Set<PartitionInfoResponse>,
) {
    companion object {
        fun fromTopicInfo(topicInfo: TopicInfo): TopicInfoResponse =
            TopicInfoResponse(topicInfo.name, topicInfo.partitionInfo.map { PartitionInfoResponse(it.id) }.toSet())
    }
}

data class PartitionInfoResponse(
    val partitionId: Int
)
