package io.conduktor.assignment.service

import io.conduktor.assignment.domain.ClusterConnection
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

@Service
class ClusterConnectionProvider {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val clusterConnections: ConcurrentHashMap<String, Pair<ClusterConnection, Admin>> = ConcurrentHashMap()

    @Synchronized
    fun createOrGetConnection(bootstrapServer: String): ClusterConnectionResult {
        if (clusterConnections.containsKey(bootstrapServer)) {
            logger.info("Connection to cluster: $bootstrapServer already exists")
            return ClusterConnectionResult.Success(clusterConnections[bootstrapServer]!!.first)
        }
        val properties = Properties()
        properties[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServer
        try {
            val adminClient = Admin.create(properties)
            val clusterId = adminClient.describeCluster().clusterId().get()
            val clusterConnection = ClusterConnection(id = bootstrapServer, clusterId = clusterId)
            clusterConnections[bootstrapServer] = Pair(clusterConnection, adminClient)
            return ClusterConnectionResult.Success(clusterConnection)
        } catch (e: Exception) {
            logger.error("Failed to create connection to cluster: $bootstrapServer", e)
            return ClusterConnectionResult.Error("Failed to create connection to cluster: $bootstrapServer, exception: ${e.message}")
        }
    }

    fun getConnection(bootstrapServer: String): Pair<ClusterConnection, Admin>? {
        if (!clusterConnections.containsKey(bootstrapServer)) {
            logger.error("Cluster: $bootstrapServer does not have an active connection")
            return null
        }
        return clusterConnections[bootstrapServer]!!
    }

    @Synchronized
    fun closeConnection(bootstrapServer: String): Boolean {
        if (!clusterConnections.containsKey(bootstrapServer)) {
            logger.warn("Cluster: $bootstrapServer does not have an active connection, nothing to close")
            return false
        }
        val pair = clusterConnections[bootstrapServer]!!
        pair.second.close()
        clusterConnections.remove(bootstrapServer)
        logger.info("Connection to cluster: $bootstrapServer closed successfully")
        return true
    }
}

sealed interface ClusterConnectionResult {
    data class Success(val clusterConnection: ClusterConnection): ClusterConnectionResult
    data class Error(val message: String): ClusterConnectionResult
}
