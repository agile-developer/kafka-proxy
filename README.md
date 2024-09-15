# Kafka Proxy Assignment

### Summary
Kafka-proxy is a simple HTTP web application that can act as a gateway to one or more Kafka clusters. It provides an API to interact with Kafka using simple HTTP endpoints. Currently, the API can:
- connect to a Kafka cluster using its URL
- create one topic with one partition, given a topic name, on a connected cluster
- retrieve topic and partition info for a cluster with an active connection
- fetch records from one topic of a connected cluster
- close an active connection to a Kafka cluster

The application is implemented as a Spring Boot web application. The reason for using Spring is to leverage its support for HTTP web-applications and dependency-injection. We **do not** make use of Spring's Kafka modules for this app.

The API is not, strictly speaking, a REST API. Rather it is an HTTP API that makes use of JSON payloads. We could just as easily have used binary, GraphQL or XML. You can think of it as an HTTP facade to fleet of Kafka clusters, which attempts to simplify communicating with Kafka.

For testing we make use of Testcontainers to run a containerized Kafka, so that we can check important use cases without mocking Kafka.

### Running the application
This project is implemented in Kotlin (`1.9.25`) and compiled for Java 21. Please ensure the target machine has Java 21 installed, and `java` is available on the `PATH`.

Once you've cloned the repository, or unzipped the source directory, change to the `kafka-proxy` folder and run the following command, to build sources:
```shell
./gradlew clean build
```
To start the application, run:
```shell
java -jar ./app/build/libs/kafka-proxy-0.0.1-SNAPSHOT.jar
```
This should start the application listening on port `8080` of your local machine. To test the endpoint use the following `curl` commands:

For local development and testing, you need to have a Kafka instance running and reachable. So you either need to have Kafka installed locally (and ZooKeeper, if you're not using KRaft), or you can start a local Kafka using Docker. One option is to use the Kafka image from Confluent Inc., which can be started with the following command:
```shell
docker run \
--name=kafka-server \
-p 9092:9092 \
-e KAFKA_NODE_ID=1 \
-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT' \
-e KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:9092' \
-e KAFKA_JMX_PORT=9101 \
-e KAFKA_JMX_HOSTNAME=localhost \
-e KAFKA_PROCESS_ROLES='broker,controller' \
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
-e KAFKA_CONTROLLER_QUORUM_VOTERS='1@localhost:29093' \
-e KAFKA_LISTENERS='PLAINTEXT://localhost:29092,CONTROLLER://localhost:29093,PLAINTEXT_HOST://0.0.0.0:9092' \
-e KAFKA_INTER_BROKER_LISTENER_NAME='PLAINTEXT' \
-e KAFKA_CONTROLLER_LISTENER_NAMES='CONTROLLER' \
-e CLUSTER_ID='MkU3OEVBNTcwNTJENDM2Qk' \
confluentinc/cp-kafka:latest
```
This will start a Kafka container (using KRaft) which is available on `localhost:9092`. Other images are available from Bitnami and Apache, which also work fine.

#### Create a connection to a running Kafka cluster
Make sure there is an active/running Kafka cluster available for which there ia DNS reachable URL. For local development `localhost:9092` will likely be the Kafka URL. Create a cluster-connection by issuing a `curl` command like this (`HTTP POST`):
```shell
curl -v \
-H 'Content-Type: application/json' \
-d '{ "bootstrapServer" : "localhost:9092" }' \
http://localhost:8080/kafka-proxy/cluster-connections
```
A successful response payload will look like this:
```json
{
  "id" : "localhost:9092",
  "clusterId": "mnW15fdSQWmvFWoIdLP-Rg"
}
```
The `id` of the returned cluster-connection is the same as its URL. I've chosen this because this URL will be unique to cluster. It also acts as natural idempotency id, when more than one request tries to create a cluster-connection for the same bootstrap URL.

#### Create a topic with a single partition, given a topic name
To create a new topic on a Kafka cluster, execute the following `curl` command (`HTTP POST`) for an active cluster-connection:
```shell
curl -v \
-H 'Content-Type: application/json' \
-H 'Accept: application/json' \
-d '{ "bootstrapServer" : "localhost:9092", "topic" : "topic-name" }' \
http://localhost:8080/kafka-proxy/topics
```
This will create a new topic with a single partition and a return a response like this:
```json
{
  "id":"eIz9KX3EQgiurWWFDPwvPw",
  "topic":"topic-name",
  "bootstrapServer":"localhost:9092"
}
```

#### Retrieve topic and partition info for a cluster-connection
To retrieve topic and partition info for a cluster, make sure you've created a cluster-connection first. Then issue the following `curl` command (`HTTP GET`)
```shell
curl -v \
http://localhost:8080/kafka-proxy/topics?bootstrapServer=localhost:9092
```
This should return a response like this:
```json
[
  {
    "name": "topic-name",
    "partitions" : [
      {
        "partitionId" : 0
      }
    ]
  }
]
```
The response is a list/array of `TopicInfoResponse` which contains the topic name and a list of `PartitionInfoResponse` which currently only has a partition-id. This can be expanded to contain more attributes, if needed.

#### Fetch available records from a given cluster and one topic
The API provides an `HTTP GET` endpoint to fetch all 'available' records from one topic of a cluster. The following `curl` will accomplish this:
```shell
curl -v \
http://localhost:8080/kafka-proxy/topics/records?bootstrapServer=localhost:9092&topic=topic-name
```
Currently, the response is simply a list/array of strings:
```json
[
  "First event",
  "Second event",
  "Third event"
]
```
This implementation reads all records from the start of the topic up to the default `max.poll.records` setting (I think it is 500). Also, it will read from all partitions of the given topic.

### TODOs - For other devs / future self
There are number of API endpoints that we can add to make our implementation more complete.

- Create a single topic with a configurable number of partitions
- Create more than one topic (each with possibly more than one partition)
- Produce records to a given topic

There are also areas for improvement, as well as revisiting certain design and implementation decisions.

- Perhaps the API endpoint to consume records should be an `HTTP POST` because we'd like to enhance the request with more parameters. Some are `startOffset`, `limit`, `partitionId`, `timeoutInMillis`. Adding all of these as `HTTP GET` query-params will become unwieldy.
- We're creating a `KafkaConsumer` (wrapped in our `OnDemandConsumer`) instance for each consume records request. For a small number of requests this is okay, but might not scale as well for a large number of concurrent requests. Creating a `KafkaConsumer` involves creating a new client connection to a cluster. We should think of how we can associate an `OnDemandConsumer` with a `ClusterConnection` and reuse it for requests to the same topic/partition.
- Currently, we are reading the records as simple strings. We need to be able to specify somehow the type (or schema) of the records we are trying to read, in the consume request. That's another reason to use `HTTP POST` as it will allow us to specify more parameters to fine tune the request.
- See how we can make use of virtual-threads to improve throughput and avoid blocking platform threads. For Spring Boot, this might be a simple configuration change to switch the underlying web-server (Jetty or Tomcat) to use virtual threads for handling requests. But we should also explore whether we want a dedicated `ExecutorService` to deal with consuming from and producing to Kafka. We'll need to try different approaches and measure the effects on performance (using something like `K6`, `bench`, `wrk` or `ab`).
- We also need to think about how to have better segregation between Kafka clusters. A problem with one Kafka cluster should not impact connections to other Kafka clusters.
- Currently, all application 'state' is in-memory. Mainly this is the cluster connections. If an instance of the kafka-proxy crashes, the connection state is lost. How and where should we store application state, such that the kafka-proxy is more robust/resilient? We could use the filesystem, a database or even a special admin Kafka cluster.
- Improve test coverage. Currently, we have end-to-end tests for the main use-cases, but we should cover the error handling scenarios as well.
- Improve the domain model and the request-response model.
- To package the application we'll create an Open Container Initiative (OCI) compliant container image.
- The application will like be deployed to a Kubernetes cluster, so we'll need to create a Helm chart for deployment.
- We need to build in observability, so we can operate the application effectively in production.
- Think about how to secure the API.
