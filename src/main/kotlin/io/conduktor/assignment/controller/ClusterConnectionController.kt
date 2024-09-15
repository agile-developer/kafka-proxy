package io.conduktor.assignment.controller

import io.conduktor.assignment.service.ClusterConnectionProvider
import io.conduktor.assignment.service.ClusterConnectionResult.Error
import io.conduktor.assignment.service.ClusterConnectionResult.Success
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.net.URI

@RestController
@RequestMapping("/kafka-proxy/cluster-connections")
class ClusterConnectionController(
    private val clusterConnectionProvider: ClusterConnectionProvider
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @PostMapping
    fun create(
        @RequestBody
        clusterConnectionRequest: ClusterConnectionRequest
    ): ResponseEntity<*> {
        val bootstrapServer = clusterConnectionRequest.bootstrapServer
        validate(bootstrapServer)?.let { errorMessage ->
            return ResponseEntity.badRequest().body(errorMessage)
        }

        return when(val result = clusterConnectionProvider.createOrGetConnection(bootstrapServer)) {
            is Success -> ResponseEntity.ok(result.clusterConnection.let {
                ClusterConnectionResponse(id = it.id, clusterId = it.clusterId)
            })
            is Error -> ResponseEntity.badRequest().body(result.message)
        }
    }

    @PostMapping("/close")
    fun close(
        @RequestBody
        clusterConnectionRequest: ClusterConnectionRequest
    ): ResponseEntity<*> {
        val bootstrapServer = clusterConnectionRequest.bootstrapServer
        validate(bootstrapServer)?.let { errorMessage ->
            return ResponseEntity.badRequest().body(errorMessage)
        }
        val isClosed = clusterConnectionProvider.closeConnection(bootstrapServer)
        return if (!isClosed) {
            ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body("Cluster: $bootstrapServer does not have an active connection, nothing to close")
        } else {
            ResponseEntity.ok("Connection to cluster: $bootstrapServer closed successfully")
        }
    }

    private fun validate(bootstrapServer: String): String? {
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

data class ClusterConnectionRequest(
    val bootstrapServer: String
)

data class ClusterConnectionResponse(
    val id: String,
    val clusterId: String
)
