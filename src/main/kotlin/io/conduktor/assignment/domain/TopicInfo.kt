package io.conduktor.assignment.domain

data class TopicInfo(
    val name: String,
    val partitionInfo: Set<PartitionInfo>
)

data class PartitionInfo(
    val id: Int
)
