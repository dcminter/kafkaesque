# JUnit 4 Support

The `kafkaesque-junit4` module provides a JUnit 4 `TestRule` for using Kafkaesque in
JUnit 4 test classes. It also works with the JUnit 5 Vintage engine for mixed projects.

## Dependency

```xml
<dependency>
    <groupId>eu.kafkaesque</groupId>
    <artifactId>kafkaesque-junit4</artifactId>
    <version>${kafkaesque.version}</version>
    <scope>test</scope>
</dependency>
```

This transitively brings in `kafkaesque-core`, so you only need the one dependency.

## Quick Start

Declare a `KafkaesqueRule` as a `@Rule` (per-method) or `@ClassRule` (per-class):

```java
public class MyKafkaTest {
    @Rule
    public KafkaesqueRule kafkaesque = new KafkaesqueRule();

    @Test
    public void shouldProduce() throws Exception {
        KafkaProducer<String, String> producer = kafkaesque.createProducer();
        producer.send(new ProducerRecord<>("topic", "key", "value")).get();
        assertThat(kafkaesque.getServer().getTotalRecordCount()).isEqualTo(1);
    }
}
```

## `@Rule` vs `@ClassRule`

| Annotation | Field | Behaviour |
|---|---|---|
| `@Rule` | Instance field | Fresh server per test method |
| `@ClassRule` | `static` field | One shared server for the entire class |

### Per-Method (fresh server each test)

```java
public class IsolatedTest {
    @Rule
    public KafkaesqueRule kafkaesqueRule = new KafkaesqueRule();

    @Test
    public void firstTest() throws Exception { /* fresh server */ }

    @Test
    public void secondTest() throws Exception { /* different fresh server */ }
}
```

### Per-Class (shared server)

```java
public class SharedTest {
    @ClassRule
    public static KafkaesqueRule kafkaesqueRule = new KafkaesqueRule();

    @Test
    public void firstTest() throws Exception { /* shared server */ }

    @Test
    public void secondTest() throws Exception { /* same server */ }
}
```

## Builder Configuration

Use the builder for custom configuration:

```java

@ClassRule
public static KafkaesqueRule kafkaesqueRule = KafkaesqueRule.builder()
        .autoCreateTopics(false)
        .topic("orders")
        .topic("shipments", 3)
        .topics(new TopicDefinition("events", 5, (short) 1))
        .build();
```

### Builder Methods

| Method | Description |
|---|---|
| `port(int)` | Port to bind to; `0` (default) for an OS-assigned ephemeral port |
| `autoCreateTopics(boolean)` | Whether unknown topics are created on produce (default: `true`) |
| `topic(String)` | Add a topic with 1 partition |
| `topic(String, int)` | Add a topic with the given partition count |
| `topics(TopicDefinition...)` | Add topics with full control (name, partitions, replication factor) |

### `TopicDefinition`

For full control over topic configuration:

```java
new TopicDefinition("orders")                       // 1 partition, replication factor 1
new TopicDefinition("orders", 5)                    // 5 partitions, replication factor 1
new TopicDefinition("orders", 5, (short) 1)         // explicit replication factor
```

## Server Access

Once the rule has started (inside a test method), access the server directly:

```java
KafkaesqueServer kafkaesque = kafkaesqueRule.getServer();
String bootstrapServers = kafkaesqueRule.getBootstrapServers();
```

The server provides methods for inspecting what has been published:

```java
kafkaesque.getTotalRecordCount();
kafkaesque.getRecords();
kafkaesque.getRecordsByTopic("orders");
kafkaesque.getRecordsByTopicAndKey("orders", "order-123");
kafkaesque.getRegisteredTopics();
```

## Producer Factory

The rule provides factory methods that create producers connected to the running server.
All created producers are automatically closed when the rule tears down.

### Default Producer

Creates a `KafkaProducer<String, String>` with `StringSerializer`, acks=all, retries=0:

```java
KafkaProducer<String, String> producer = kafkaesqueRule.createProducer();
producer.send(new ProducerRecord<>("topic", "key", "value")).get();
```

### Typed Producer

Specify custom serializer classes:

```java
KafkaProducer<String, Foo> producer = kafkaesqueRule.createProducer(
    StringSerializer.class, FooSerializer.class);
```

### Custom Properties

Full control — only `bootstrap.servers` is injected automatically:

```java
Properties props = new Properties();
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.ACKS_CONFIG, "1");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

KafkaProducer<String, String> producer = kafkaesqueRule.createProducer(props);
```

## Consumer Factory

The rule provides factory methods that create consumers connected to the running server.
All created consumers are automatically closed when the rule tears down.

### Default Consumer (with optional subscription)

Creates a `KafkaConsumer<String, String>` with `StringDeserializer`, random group ID,
auto-offset-reset=earliest:

```java
// Pre-subscribed
KafkaConsumer<String, String> consumer = kafkaesqueRule.createConsumer("orders", "notifications");

// Manual subscription
KafkaConsumer<String, String> consumer = kafkaesqueRule.createConsumer();
consumer.subscribe(List.of("orders"));
```

### Typed Consumer

Specify custom deserializer classes:

```java
KafkaConsumer<String, Foo> consumer = kafkaesqueRule.createConsumer(
    StringDeserializer.class, FooDeserializer.class, "orders");
```

### Custom Properties

Full control — only `bootstrap.servers` is injected automatically:

```java
Properties props = new Properties();
props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

KafkaConsumer<String, String> consumer = kafkaesqueRule.createConsumer(props);
consumer.subscribe(List.of("orders"));
```

## Resource Management

All producers and consumers created via the factory methods are tracked and automatically
closed when the rule tears down:

- For `@Rule` — after each test method
- For `@ClassRule` — after the last test in the class

You can also close them explicitly with try-with-resources if you prefer deterministic
cleanup within a test method.

## Full Example

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

## JUnit 5 Vintage Engine

If your project has migrated to JUnit 5 but you still have JUnit 4 tests, the `kafkaesque-junit4` module works with the 
JUnit 5 Vintage engine. Add the Vintage engine to your test dependencies:

```xml
<dependency>
    <groupId>org.junit.vintage</groupId>
    <artifactId>junit-vintage-engine</artifactId>
    <version>${junit.version}</version>
    <scope>test</scope>
</dependency>
```

Your JUnit 4 tests using `KafkaesqueRule` with `@Rule`/`@ClassRule` will run alongside JUnit 5 tests without any 
changes.