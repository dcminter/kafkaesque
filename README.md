# Kafkaesque

A library for mocking [Apache Kafka](https://kafka.apache.org/) dependencies in a realistic way.

By re-using the Kafka client library datatypes, Kafkaesque is compatible with the Kafka TCP wire-protocol but without 
the startup overhead required to launch the real Kafka brokers.

## Status

I'd call this a "potentially useful beta" - give it a whirl if you think it might be handy, but you'll need to 
build it yourself!

Kafkaesque is currently compatible with the **3.9.0** Apache Client library.

## Why not just use real Kafka?

While running Kafka itself (perhaps within [TestContainers](https://testcontainers.com/modules/kafka/)) is a perfectly reasonable approach, it does have 
some drawbacks - depending on how you configure and launch it, it can be slow, perhaps taking multiple seconds to 
start up in a naiive configuration. If you're currently using Kafka in your integration tests and have no problems, 
then Kafkaesque is probably not the tool for you.

If you're finding your Kafka tests are very slow (particularly if they launch large numbers of Kafka instances during 
the test lifecycle), or you want more control over the exact behaviours you're testing for, then Kafkaesque might be 
a good fit. It also might work for you if running Kafka inside testcontainers creates a dependency on Docker that 
would otherwise be unnecessary.

Note that if your tests are very slow because you're inserting `sleep` statements into otherwise fragile tests of 
asynchronous behaviour, then you might alternatively/additionally want to investigate the 
excellent [Awaitility library](http://www.awaitility.org/). Also, if you need 100% guaranteed compatibility with real Kafka in your
integration tests, you should do so - Kafkaesque cannot (and doesn't try to) be 100% compatible in every way.

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

## Example

This example assumes an existing application under test that consumes from an `orders` topic and
publishes a confirmation to a `notifications` topic. Here's how you might write the test for JUnit 5
using Kafkaesque:

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

## Further documentation

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
