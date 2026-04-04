# Event Storage and Retrieval Feature

## Overview

Kafkaesque now stores all published events in memory and provides convenient methods to retrieve them directly from the `KafkaesqueServer` instance. This allows test code to verify that messages were published correctly without needing to set up a Kafka consumer.

## New Classes

### `StoredRecord`
A record type that captures all metadata about a published event:
- `topic` - the topic name
- `partition` - the partition index
- `offset` - the assigned offset within the partition
- `timestamp` - the publication timestamp (epoch milliseconds)
- `key` - the message key (nullable)
- `value` - the message value (nullable)
- `headers` - the record headers (never null; empty list if none were set)

### `EventStore`
Thread-safe in-memory storage for published events with the following features:
- Automatic offset assignment per partition (starting from 0)
- Organized by topic and partition
- Multiple retrieval methods with filtering capabilities
- Record counting and topic listing

## Retrieval Methods on `KafkaesqueServer`

### Basic Retrieval

```java
// Get all records from a specific topic and partition
List<StoredRecord> getRecords(String topic, int partition)

// Get all records from a specific topic (all partitions)
List<StoredRecord> getRecordsByTopic(String topic)

// Get all records across all topics and partitions
List<StoredRecord> getAllRecords()
```

### Filtered Retrieval

```java
// Get records by topic and key
List<StoredRecord> getRecordsByTopicAndKey(String topic, String key)

// Find records matching a predicate
List<StoredRecord> findRecords(Predicate<StoredRecord> predicate)
```

### Statistics

```java
// Get total record count
long getTotalRecordCount()

// Get record count for a topic
long getRecordCount(String topic)

// Get record count for a specific topic and partition
long getRecordCount(String topic, int partition)

// Get all known topics
List<String> getKnownTopics()
```

### Maintenance

```java
// Clear all stored records (useful for test cleanup)
void clearRecords()
```

## Usage Examples

### Basic Publishing and Retrieval

```java
// Start Kafkaesque server
try (var server = new KafkaesqueServer("localhost", 0)) {
    server.start();

    // Publish some messages using Kafka producer
    var producer = new KafkaProducer<String, String>(props);
    producer.send(new ProducerRecord<>("my-topic", "key1", "value1"));
    producer.send(new ProducerRecord<>("my-topic", "key2", "value2"));
    producer.flush();

    // Retrieve and verify directly from server
    var records = server.getRecordsByTopic("my-topic");
    assertEquals(2, records.size());
    assertEquals("value1", records.get(0).value());
    assertEquals("value2", records.get(1).value());
}
```

### Filtering by Key

```java
// Publish multiple messages with different keys
producer.send(new ProducerRecord<>("events", "user-123", "login"));
producer.send(new ProducerRecord<>("events", "user-456", "logout"));
producer.send(new ProducerRecord<>("events", "user-123", "purchase"));
producer.flush();

// Retrieve only messages for user-123
var user123Events = server.getRecordsByTopicAndKey("events", "user-123");
assertEquals(2, user123Events.size());
```

### Custom Filtering

```java
// Find all records containing "error" in the value
var errorRecords = server.findRecords(record ->
    record.value() != null && record.value().contains("error")
);

// Find all records from the last hour
var cutoff = Instant.now().minus(Duration.ofHours(1));
var recentRecords = server.findRecords(record ->
    record.timestampAsInstant().isAfter(cutoff)
);
```

### Test Cleanup

```java
@BeforeEach
void setUp() throws IOException {
    server = new KafkaesqueServer("localhost", 0);
    server.start();
}

@AfterEach
void tearDown() {
    server.clearRecords(); // Clear between tests
    server.close();
}
```

## Implementation Details

### Offset Assignment
- Each partition maintains its own offset counter starting from 0
- Offsets are assigned sequentially and atomically
- Different partitions have independent offset sequences

### Thread Safety
- The `EventStore` is fully thread-safe
- Uses `ConcurrentHashMap` for partition storage
- Synchronized access to record lists
- Returns immutable copies from all retrieval methods

### Record Storage
- Records are stored in the order they are received
- All metadata from the Kafka protocol is preserved
- Keys and values are extracted from the Kafka record batches as strings

## Testing

### Unit Tests
- `EventStoreTest` - Comprehensive tests for the EventStore class
  - 17 test cases covering all functionality
  - Tests for offset assignment, filtering, counting, and edge cases

### Event Retrieval Tests
- `KafkaesqueServerEventRetrievalTest` - Tests with real Kafka producer
  - Located in `kafkaesque-core/src/test/` alongside the unit tests
  - Tests publishing and retrieval workflows
  - Tests filtering and counting
  - Tests with multiple topics and partitions
