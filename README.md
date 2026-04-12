# Kafkaesque

![Build](https://github.com/dcminter/kafkaesque/actions/workflows/build_on_branch_or_pr.yml/badge.svg)

A library for mocking [Apache Kafka](https://kafka.apache.org/) dependencies in a realistic way.

By re-using the Kafka client library datatypes, Kafkaesque is compatible with the Kafka TCP wire-protocol but without 
the startup overhead required to launch the real Kafka brokers.

## Status

I'd call this a "potentially useful beta" - give it a whirl if you think it might be handy!

Kafkaesque is currently compatible with the **3.9.1** Apache Client library.

## Why not just use real Kafka?

While running Kafka itself (perhaps within [TestContainers](https://testcontainers.com/modules/kafka/)) is a perfectly 
reasonable approach, it does have some drawbacks - depending on how you configure and launch it, it can be slow, perhaps 
taking multiple seconds to start up in a naiive configuration. If you're currently using Kafka in your integration tests 
and have no problems, then Kafkaesque is probably *not* the tool for you.

If, however, you're finding your Kafka tests are very slow (particularly if they launch large numbers of Kafka instances 
during the test lifecycle), or you want more control over the exact behaviours you're testing for, then Kafkaesque might 
be a good fit. It also might work for you if running Kafka inside testcontainers creates a dependency on Docker that 
would otherwise be unnecessary.

Note that if your tests are very slow because you're inserting `sleep` statements into otherwise fragile tests of 
asynchronous behaviour, then you might alternatively/additionally want to investigate the excellent 
[Awaitility library](http://www.awaitility.org/). Also, if you need 100% guaranteed compatibility with real Kafka in 
your integration tests, you should stick with real Kafka - Kafkaesque cannot (and doesn't try to) be 100% compatible in 
every way.

## Examples

Kafkaesque provides direct support for JUnit 4 and JUnit 5. The following examples assume an existing application
under test that consumes from an `orders` topic and then publishes a confirmation to a `notifications` topic.

### JUnit 5 (Jupiter)

Here's how you might write the test for JUnit 5 using Kafkaesque:

```java
@Kafkaesque(topics = {
    @KafkaesqueTopic(name = "orders"),
    @KafkaesqueTopic(name = "notifications")
})
class OrderNotificationServiceTest {

    @Test
    void shouldSendNotificationWhenOrderIsPlaced(
            final KafkaesqueServer kafkaesque,
            @KafkaesqueProducer final KafkaProducer<String, String> producer) throws Exception {

        // Start the application under test, pointed at our mock Kafka
        var application = new OrderNotificationService(kafkaesque.getBootstrapServers());
        application.start();

        // Simulate an incoming order
        producer.send(new ProducerRecord<>("orders", "order-123",
                """
                { "customer": "Alice", "item": "Kafka In Action", "quantity": 1}
                """)).get();

        // Verify that the service produced a notification in response
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            var notifications = kafkaesque.getRecordsByTopic("notifications");
            assertThat(notifications).hasSize(1);
            assertThat(notifications.getFirst().key()).isEqualTo("order-123");
            assertThat(notifications.getFirst().value()).contains("Alice");
        });

        application.stop();
    }
}
```

* The `@Kafkaesque` annotation spins up an in-process mock that speaks the real Kafka wire protocol
* The topics and producer are created via Kafkaesque's annotations
* Kafkaesque exposes a `getRecordsByTopic(...)` method on the server that lets you inspect what was published without wiring up a consumer
* The application under test uses standard Kafka clients and has no knowledge of Kafkaesque

### JUnit 4

The same scenario using JUnit 4's `@ClassRule`:

```java
public class OrderNotificationServiceTest {

    @ClassRule
    public static KafkaesqueRule kafkaesqueRule = KafkaesqueRule.builder()
            .topics(
                    new TopicDefinition("orders"),
                    new TopicDefinition("notifications"))
            .build();

    @Test
    public void shouldSendNotificationWhenOrderIsPlaced() throws Exception {
        var application = new OrderNotificationService(kafkaesqueRule.getBootstrapServers());
        application.start();

        KafkaProducer<String, String> producer = kafkaesqueRule.createProducer();
        producer.send(new ProducerRecord<>("orders", "order-123",
                "{ \"customer\": \"Alice\", \"item\": \"Kafka In Action\", \"quantity\": 1}")).get();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            var notifications = kafkaesqueRule.getServer().getRecordsByTopic("notifications");
            assertThat(notifications).hasSize(1);
            assertThat(notifications.getFirst().key()).isEqualTo("order-123");
            assertThat(notifications.getFirst().value()).contains("Alice");
        });

        application.stop();
    }
}
```

* `KafkaesqueRule` is a JUnit 4 `TestRule` and can be used as an `@Rule` (per-method) or `@ClassRule` (per-class)
* Topics can be configured via a builder pattern on the `KafkaesqueRule`
* Factory methods (`createProducer()`, `createConsumer(...)`) are provided to make it simpler to create clients connected to the mock server

### Standalone Docker Container

Kafkaesque can also run as a standalone service via Docker, useful during local development or
in CI pipelines. A pre-built image is published to GitHub Container Registry on each release:

```bash
docker pull ghcr.io/dcminter/kafkaesque:latest
docker run -p 9092:9092 ghcr.io/dcminter/kafkaesque:latest
```

Your Kafka clients can then connect to `localhost:9092`. See [the standalone guide](docs/STANDALONE.md)
for configuration options and Docker Compose examples.

# Features

  * Produce and fetch using standard Kafka clients
  * Multi-partition topics with configurable partition counts
  * Transactions (including `READ_COMMITTED` / `READ_UNCOMMITTED`)
  * Consumer groups with partition rebalancing
  * JUnit 5 (`@Kafkaesque` server instantiation per-class or per-method, `@KafkaesqueTopic` topic creation, `@KafkaesqueProducer`, `@KafkaesqueConsumer` injection)
  * JUnit 4 (`KafkaesqueRule` usable as `@Rule` or `@ClassRule`, builder for topic configuration, factory method for producer/consumer creation)
  * Direct record inspection (no consumer required)
  * Admin client operations
  * Log compaction / retention
  * Auto-topic-creation (optional)
  * Event listeners (record published, topic created, transaction completed)
  * Record headers
  * Server-side compression (gzip, snappy, lz4, zstd) configurable per topic
  * Server-side idempotency for producers (dedupe on retry, per-partition sequence tracking)
  * Offset commit and reset (`earliest`, `latest`)
  * Standalone executable Jar or Docker container

## Installation

Kafkaesque is published to Maven Central. Add the dependency for your test framework:

**JUnit 5 (Jupiter):**

```xml
<dependency>
    <groupId>eu.kafkaesque</groupId>
    <artifactId>kafkaesque-junit5</artifactId>
    <version>0.1.0</version>
    <scope>test</scope>
</dependency>
```

**JUnit 4:**

```xml
<dependency>
    <groupId>eu.kafkaesque</groupId>
    <artifactId>kafkaesque-junit4</artifactId>
    <version>0.1.0</version>
    <scope>test</scope>
</dependency>
```

**Or use the BOM for dependency management:**

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>eu.kafkaesque</groupId>
            <artifactId>kafkaesque-bom</artifactId>
            <version>0.1.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

## Building and testing

The build tool is [Maven](https://maven.apache.org/) and we're using [Maven Wrapper](https://maven.apache.org/tools/wrapper/)
so to build and run the test suite:

```bash
$ ./mvnw clean verify
```

Or to install into your local artifact repository:

```bash
$ ./mvnw clean install
```

To build the Docker container and run it locally from source:

```bash
docker build -t kafkaesque .
docker run -p 9092:9092 kafkaesque
```

## Further documentation

  * See [the JUnit 5 guide](docs/JUNIT5.md) for full details of the JUnit 5 (Jupiter) annotations and extension.
  * See [the JUnit 4 guide](docs/JUNIT4.md) for full details of the JUnit 4 rule, builder, and Vintage engine compatibility.
  * See [the standalone guide](docs/STANDALONE.md) for running Kafkaesque as a Docker container or executable JAR.
  * See [the listener documentation](docs/LISTENERS.md) for details of how to get various callbacks without using Kafka client libraries.
  * See [the event storage summary](docs/EVENT_STORAGE_SUMMARY.md) for details of the internal representation of events etc.
  * See [the future directions documentation](docs/FUTURE.md) for a sketch of features I plan to add to Kafkaesque.

## License & Development

The software [is licensed under the Apache License, Version 2.0](LICENSE.txt)

This software is designed to support projects making extensive use of Apache Kafka. It depends on
Apache Kafka libraries for its wire-protocol types, and it therefore makes sense to release it under the
same license.

## AI Declaration

Large parts of this software were developed using [Claude Code](https://code.claude.com/)
