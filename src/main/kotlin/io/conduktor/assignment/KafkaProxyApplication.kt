package io.conduktor.assignment

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaProxyApplication

fun main(args: Array<String>) {
	runApplication<KafkaProxyApplication>(*args)
}
