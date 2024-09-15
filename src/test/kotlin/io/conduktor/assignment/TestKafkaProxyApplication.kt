package io.conduktor.assignment

import org.springframework.boot.fromApplication
import org.springframework.boot.with

fun main(args: Array<String>) {
	fromApplication<KafkaProxyApplication>().with(TestcontainersConfiguration::class).run(*args)
}
