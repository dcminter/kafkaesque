# Listeners

Kafkaesque supports registering listeners that receive callbacks when specific server events occur.
This allows test code to react to events as they happen rather than polling retrieval methods.

## Available Listener Types

We're open to adding more listeners if there's demand for them; for now we have the following ones:

### RecordPublishedListener

Called whenever a record is stored on the server, whether as part of a transaction or not.

```java
@FunctionalInterface
public interface RecordPublishedListener {
    void onRecordPublished(StoredRecord record);
}
```

### TopicCreatedListener

Called when a new topic is registered on the server (via the admin API or `createTopic()`).
Not fired on duplicate creation of an already-existing topic.

```java
@FunctionalInterface
public interface TopicCreatedListener {
    void onTopicCreated(String topicName);
}
```

### TransactionCompletedListener

Called when a transaction is committed or aborted.

```java
@FunctionalInterface
public interface TransactionCompletedListener {
    void onTransactionCompleted(String transactionalId, boolean committed);
}
```

## Registering Listeners

Listeners are registered on the `KafkaesqueServer` instance via `add*Listener()` methods.
They can be registered at any point after server construction (before or after `start()`).

### Example

This example demonstrates listening for Records published to a specific topic:

```java
try (var server = new KafkaesqueServer("localhost", 0)) {
    var orderEvents = new CopyOnWriteArrayList<StoredRecord>();
    var latch = new CountDownLatch(2); // expect 2 "orders" records

    // Register a listener that captures records for the "orders" topic
    server.addRecordPublishedListener(record -> {
        if ("orders".equals(record.topic())) {
            orderEvents.add(record);
            latch.countDown();
        }
    });

    server.start();

    // Use a Kafka producer to publish records
    var props = new Properties();
    props.put("bootstrap.servers", server.getBootstrapServers());
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    try (var producer = new KafkaProducer<String, String>(props)) {
        producer.send(new ProducerRecord<>("orders", "order-1", "created")).get();
        producer.send(new ProducerRecord<>("other-topic", "key", "value")).get();
        producer.send(new ProducerRecord<>("orders", "order-2", "shipped")).get();
    }

    // Wait for the async listener callbacks before asserting
    assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for listener callbacks");

    // The listener captured only the "orders" records
    assertEquals(2, orderEvents.size());
    assertEquals("order-1", orderEvents.get(0).key());
    assertEquals("order-2", orderEvents.get(1).key());
}
```

## Asynchronous Dispatch

Listener callbacks are **not** invoked on the server's NIO event-loop thread. Instead, the
event loop enqueues lightweight notification objects and returns immediately. A dedicated 
daemon thread (`kafkaesque-listener`) consumes from the queue and invokes the registered 
listeners. This means:

- **The NIO event loop is never blocked by listener execution.** Slow or misbehaving
  listeners cannot stall protocol processing or cause client timeouts.
- **Notifications are delivered in order.** The single consumer thread processes the queue
  sequentially, so listeners observe events in the same order they occurred on the server.
- **Delivery is asynchronous.** There is a small delay between a record being stored and the
  listener being called. In tests, use a `CountDownLatch` or similar mechanism if you need to
  wait for a callback before making assertions.

## Thread Safety

- Listeners are invoked on the `kafkaesque-listener` thread, not the test thread or the NIO
  thread. Use a thread-safe collection (e.g. `CopyOnWriteArrayList`) if the listener
  accumulates results that are read from the test thread.
- If a listener throws an exception it is caught and logged; other registered listeners and
  server operations are not affected.
- The consumer thread is shut down automatically when the server is closed. Any notifications
  already in the queue are delivered before the thread terminates.
