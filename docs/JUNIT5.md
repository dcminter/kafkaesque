# JUnit 5 (Jupiter) Support

The `kafkaesque-junit5` module provides annotations and a JUnit 5 extension for using
Kafkaesque in Jupiter test classes.

## Dependency

```xml
<dependency>
    <groupId>eu.kafkaesque</groupId>
    <artifactId>kafkaesque-junit5</artifactId>
    <version>${kafkaesque.version}</version>
    <scope>test</scope>
</dependency>
```

This transitively brings in `kafkaesque-core`, so you only need the one dependency.

## Quick Start

Annotate your test class with `@Kafkaesque` and inject a `KafkaesqueServer` parameter:

```java
@Kafkaesque
class MyKafkaTest {

    @Test
    void shouldProduceAndConsume(KafkaesqueServer kafkaesque) throws Exception {
        String bootstrapServers = kafkaesque.getBootstrapServers();
        // configure Kafka clients with bootstrapServers ...
    }
}
```

## The `@Kafkaesque` Annotation

`@Kafkaesque` can be placed on a **class** or an individual **test method**.

### Lifecycle

When placed on a class, the `lifecycle` attribute controls server lifetime:

| Value | Behaviour |
|---|---|
| `PER_CLASS` (default) | One server for the entire class, shared by all tests |
| `PER_METHOD` | A fresh server for each test method |

```java
@Kafkaesque(lifecycle = Lifecycle.PER_METHOD)
class IsolatedTest {
    @Test
    void eachTestGetsAFreshServer(KafkaesqueServer kafkaesque) { ... }
}
```

When placed on an individual **test method**, that method always receives its own dedicated
server, regardless of any class-level annotation:

```java
@Kafkaesque
class MixedTest {

    @Test
    void usesSharedServer(KafkaesqueServer kafkaesque) { ... }

    @Kafkaesque  // gets its own server
    @Test
    void hasItsOwnServer(KafkaesqueServer kafkaesque) { ... }
}
```

### Explicit Port

By default, the server binds to an OS-assigned ephemeral port. Set the `port` attribute to
bind to a specific port:

```java
@Kafkaesque(port = 19092)
class FixedPortTest {
    @Test
    void serverRunsOnPort19092(KafkaesqueServer kafkaesque) throws Exception {
        assertThat(kafkaesque.getPort()).isEqualTo(19092);
    }
}
```

This is useful when your application under test needs to connect to a predictable address.

### Auto-Create Topics

By default, producing to an unknown topic creates it automatically (mirroring real Kafka's
default). Set `autoCreateTopics = false` to disable this:

```java
@Kafkaesque(autoCreateTopics = false)
class StrictTopicTest { ... }
```

### Pre-Created Topics

Declare topics that should exist before any test runs:

```java
@Kafkaesque(
    autoCreateTopics = false,
    topics = {
        @KafkaesqueTopic(name = "orders"),
        @KafkaesqueTopic(name = "shipments", numPartitions = 3)
    }
)
class OrderTest { ... }
```

When both a class and a method carry `@Kafkaesque`, topics from **both** are created — class
topics first, then method topics.

## Producer Injection

Annotate a `KafkaProducer` parameter with `@KafkaesqueProducer` to have one injected
automatically. It is connected to the running server and closed at the end of the test:

```java
@Kafkaesque(topics = @KafkaesqueTopic(name = "orders"))
class ProducerTest {

    @Test
    void shouldProduce(
            @KafkaesqueProducer KafkaProducer<String, String> producer) throws Exception {
        producer.send(new ProducerRecord<>("orders", "key", "value")).get();
    }
}
```

### Attributes

| Attribute | Default | Description |
|---|---|---|
| `keySerializer` | `StringSerializer` | Serializer for record keys |
| `valueSerializer` | `StringSerializer` | Serializer for record values |
| `acks` | `"all"` | Acknowledgement mode |
| `retries` | `0` | Retry count |
| `idempotent` | `false` | Enable exactly-once semantics |
| `transactionalId` | `""` | Non-empty enables transactional production |

### Transactional Producers

```java
@Test
void shouldCommitTransaction(
        @KafkaesqueProducer(transactionalId = "tx-1", idempotent = true)
        KafkaProducer<String, String> producer) throws Exception {
    producer.initTransactions();
    producer.beginTransaction();
    producer.send(new ProducerRecord<>("topic", "key", "value")).get();
    producer.commitTransaction();
}
```

## Consumer Injection

Annotate a `KafkaConsumer` parameter with `@KafkaesqueConsumer`:

```java
@Test
void shouldConsume(
        @KafkaesqueConsumer(topics = "orders")
        KafkaConsumer<String, String> consumer) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
    assertThat(records).isNotEmpty();
}
```

When `topics` is non-empty, the consumer is pre-subscribed. When empty (default), you must
call `subscribe()` or `assign()` yourself.

### Attributes

| Attribute | Default | Description |
|---|---|---|
| `topics` | `{}` | Topics to subscribe to (empty = manual subscription) |
| `groupId` | `""` | Consumer group ID (empty = random UUID per test) |
| `keyDeserializer` | `StringDeserializer` | Deserializer for record keys |
| `valueDeserializer` | `StringDeserializer` | Deserializer for record values |
| `autoOffsetReset` | `"earliest"` | Offset reset policy |
| `isolationLevel` | `READ_UNCOMMITTED` | Transaction isolation level |

### Isolation Level

Use `READ_COMMITTED` to only see committed transactional records:

```java
@Test
void shouldReadOnlyCommitted(
        @KafkaesqueConsumer(topics = "orders", isolationLevel = READ_COMMITTED)
        KafkaConsumer<String, String> consumer) { ... }
```

## Full Example

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

        var application = new OrderNotificationService(kafkaesque.getBootstrapServers());
        application.start();

        producer.send(new ProducerRecord<>("orders", "order-123",
                """
                { "customer": "Alice", "item": "Kafka In Action", "quantity": 1}
                """)).get();

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
