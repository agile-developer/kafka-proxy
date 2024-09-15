package io.conduktor.assignment.service

import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.mockkStatic
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.Uuid
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.Properties

class ClusterConnectionProviderTest {

    private val adminClient = mockk<Admin>()
    private val clusterId = Uuid.randomUuid().toString()
    private val bootstrapServer = "localhost:9092"

    @Test
    fun `should return success when 'Admin' instance is created`() {
        mockkStatic(Admin::class) {
            // arrange
            every { Admin.create(any(Properties::class)) } returns adminClient
            every { adminClient.describeCluster().clusterId().get() } returns clusterId

            // act
            val result = ClusterConnectionProvider().createOrGetConnection(bootstrapServer)

            // assert
            assertThat(result).isInstanceOf(ClusterConnectionResult.Success::class.java)
            val success = result as ClusterConnectionResult.Success
            assertThat(success.clusterConnection.id).isEqualTo(bootstrapServer)
            assertThat(success.clusterConnection.clusterId).isEqualTo(clusterId)
        }
    }

    @Test
    fun `should return error when 'Admin' instance creation throws an exception`() {
        mockkStatic(Admin::class) {
            // arrange
            val exception = RuntimeException("Something went wrong")
            every { Admin.create(any(Properties::class)) } throws exception

            // act
            val result = ClusterConnectionProvider().createOrGetConnection(bootstrapServer)

            // assert
            assertThat(result).isInstanceOf(ClusterConnectionResult.Error::class.java)
            val error = result as ClusterConnectionResult.Error
            assertThat(error.message).isEqualTo("Failed to create connection to cluster: $bootstrapServer, exception: ${exception.message}")
        }
    }

    @Test
    fun `should return null when getting a connection called without creating it first`() {
        // arrange / act
        val result = ClusterConnectionProvider().getConnection(bootstrapServer)

        // assert
        assertThat(result).isNull()
    }

    @Test
    fun `should return a connection instance when getting it after creating it`() {
        mockkStatic(Admin::class) {
            // arrange
            every { Admin.create(any(Properties::class)) } returns adminClient
            every { adminClient.describeCluster().clusterId().get() } returns clusterId

            // act
            val provider = ClusterConnectionProvider()
            provider.createOrGetConnection(bootstrapServer)
            val result = provider.getConnection(bootstrapServer)

            // assert
            assertThat(result).isNotNull()
            assertThat(result).isInstanceOf(Pair::class.java)
            val clusterConnection = (result as Pair).first
            assertThat(clusterConnection.id).isEqualTo(bootstrapServer)
            assertThat(clusterConnection.clusterId).isEqualTo(clusterId)
        }
    }

    @Test
    fun `should return false when trying to close a non-existent connection`() {
        // arrange / act
        val result = ClusterConnectionProvider().closeConnection(bootstrapServer)

        // assert
        assertThat(result).isFalse()
    }

    @Test
    fun `should return true when closing an existing connection`() {
        mockkStatic(Admin::class) {
            // arrange
            every { Admin.create(any(Properties::class)) } returns adminClient
            every { adminClient.describeCluster().clusterId().get() } returns clusterId
            justRun { adminClient.close() }

            // act
            val provider = ClusterConnectionProvider()
            provider.createOrGetConnection(bootstrapServer)
            val result = provider.closeConnection(bootstrapServer)

            // assert
            assertThat(result).isTrue()
        }
    }
}
