package io.conduktor.assignment

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.conduktor.assignment.controller.ClusterConnectionResponse
import io.conduktor.assignment.controller.CreateTopicResponse
import io.conduktor.assignment.controller.TopicInfoResponse
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders
import org.testcontainers.containers.KafkaContainer
import java.util.Properties

@Import(TestcontainersConfiguration::class)
@SpringBootTest
@AutoConfigureMockMvc
class KafkaProxyApplicationTests {

//	@Test
//	fun contextLoads() {
//	}

    @Autowired
    private lateinit var kafka: KafkaContainer

    @Autowired
    private lateinit var mockMvc: MockMvc

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    private val mapper = jacksonObjectMapper()

    @Test
    fun `should create an admin client instance and return it as a cluster connection`() {
        // arrange
        val bootstrapServer = kafka.bootstrapServers
        val connectionRequest = """
			{
				"bootstrapServer" : "$bootstrapServer"
			}
		""".trimIndent()

        // act
        val result = mockMvc.perform(
            MockMvcRequestBuilders.post("/kafka-proxy/cluster-connections")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .content(connectionRequest)
        ).andReturn()

        // assert
        assertThat(result.response.status).isEqualTo(200)
        val responseString = result.response.contentAsString
        assertThat(responseString).isNotBlank()
        val response = mapper.readValue(responseString, ClusterConnectionResponse::class.java)
        assertThat(response.id).isEqualTo(bootstrapServer)
        assertThat(response.clusterId).isNotBlank()
    }

    @Test
    fun `should return topic and partition info for a cluster connection`() {
        // arrange
        val bootstrapServer = kafka.bootstrapServers
        val connectionRequest = """
			{
				"bootstrapServer" : "$bootstrapServer"
			}
		""".trimIndent()
        val topic = "test-topic1"
        val properties = Properties()
        properties[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServer
        val adminClient = Admin.create(properties)
        val createTopicsResult = adminClient.createTopics(mutableListOf(NewTopic(topic, 1, 1)))
        createTopicsResult.topicId(topic).get()

        // act
        mockMvc.perform(
            MockMvcRequestBuilders.post("/kafka-proxy/cluster-connections")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .content(connectionRequest)
        ).andReturn()

        val result = mockMvc.perform(
            MockMvcRequestBuilders.get("/kafka-proxy/topics?bootstrapServer=$bootstrapServer")
        ).andReturn()

        // assert
        assertThat(result.response.status).isEqualTo(200)
        val responseString = result.response.contentAsString
        assertThat(responseString).isNotBlank()
        val response = mapper.readValue(responseString, Array<TopicInfoResponse>::class.java)
        assertThat(response[0].name).isEqualTo(topic)
        assertThat(response[0].partitions.size).isEqualTo(1)
    }

    @Test
    fun `should create a new single partition topic info for an active cluster connection`() {
        // arrange
        val bootstrapServer = kafka.bootstrapServers
        val connectionRequest = """
			{
				"bootstrapServer" : "$bootstrapServer"
			}
		""".trimIndent()
        val topic = "new-topic"
        val createTopicRequest = """
			{
				"bootstrapServer" : "$bootstrapServer",
                "topic" : "$topic"
			}
		""".trimIndent()

        // act
        mockMvc.perform(
            MockMvcRequestBuilders.post("/kafka-proxy/cluster-connections")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .content(connectionRequest)
        ).andReturn()

        val result = mockMvc.perform(
            MockMvcRequestBuilders.post("/kafka-proxy/topics")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .content(createTopicRequest)
        ).andReturn()

        // assert
        assertThat(result.response.status).isEqualTo(200)
        val responseString = result.response.contentAsString
        assertThat(responseString).isNotBlank()
        val response = mapper.readValue(responseString, CreateTopicResponse::class.java)
        assertThat(response.id).isNotBlank()
        assertThat(response.topic).isEqualTo(topic)
        assertThat(response.bootstrapServer).isEqualTo(bootstrapServer)
    }

    @Test
    fun `should return records produced on a given topic`() {
        // arrange
        val bootstrapServer = kafka.bootstrapServers
        val connectionRequest = """
			{
				"bootstrapServer" : "$bootstrapServer"
			}
		""".trimIndent()
        val topic = "test-topic2"
        val properties = Properties()
        properties[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServer
        val adminClient = Admin.create(properties)
        val createTopicsResult = adminClient.createTopics(mutableListOf(NewTopic(topic, 1, 1)))
        createTopicsResult.topicId(topic).get()
        kafkaTemplate.send(topic, "key1", "data1")
        kafkaTemplate.send(topic, "key2", "data2")
        kafkaTemplate.send(topic, "key3", "data3")

        // act
        mockMvc.perform(
            MockMvcRequestBuilders.post("/kafka-proxy/cluster-connections")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .content(connectionRequest)
        ).andReturn()

        val result = mockMvc.perform(
            MockMvcRequestBuilders.get("/kafka-proxy/topics/records?bootstrapServer=$bootstrapServer&topic=$topic")
        ).andReturn()

        // assert
        assertThat(result.response.status).isEqualTo(200)
        val responseString = result.response.contentAsString
        assertThat(responseString).isNotBlank()
        val response = mapper.readValue(responseString, Array<String>::class.java)
        assertThat(response[0]).isEqualTo("data1")
        assertThat(response[1]).isEqualTo("data2")
        assertThat(response[2]).isEqualTo("data3")
    }

    @Test
    fun `should successfully close an active cluster connection`() {
        // arrange
        val bootstrapServer = kafka.bootstrapServers
        val connectionRequest = """
			{
				"bootstrapServer" : "$bootstrapServer"
			}
		""".trimIndent()

        // act
        mockMvc.perform(
            MockMvcRequestBuilders.post("/kafka-proxy/cluster-connections")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .content(connectionRequest)
        ).andReturn()

        // assert
        val result = mockMvc.perform(
            MockMvcRequestBuilders.post("/kafka-proxy/cluster-connections/close")
                .contentType(MediaType.APPLICATION_JSON)
                .content(connectionRequest)
        ).andReturn()

        assertThat(result.response.status).isEqualTo(200)
        val responseString = result.response.contentAsString
        assertThat(responseString).isNotBlank()
        assertThat(responseString).isEqualTo("Connection to cluster: $bootstrapServer closed successfully")
    }
}
