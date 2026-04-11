# Event Storage and Retrieval Feature

## Overview

Kafkaesque stores all published events in memory and provides convenient methods to retrieve them directly from the `KafkaesqueServer` instance. This allows test code to verify that messages were published correctly without needing to set up a Kafka consumer.

The storage layer also supports Kafka's transactional semantics, including pending/committed/aborted record states and isolation-level-aware retrieval.

## Storage Classes

### `StoredRecord`
An immutable record type that captures all metadata about a published event:
- `topic` - the topic name
- `partition` - the partition index
- `offset` - the assigned offset within the partition
- `timestamp` - the publication timestamp (epoch milliseconds)
- `headers` - the record headers (never null; empty list if none were set)
- `key` - the message key (nullable)
- `value` - the message value (nullable)

Provides `timestampAsInstant()` for time-based operations.

### `RecordData`
A context record used as input to `EventStore` storage methods, avoiding long parameter lists:
- `topic`, `partition`, `timestamp`, `key`, `value`, `headers`

### `EventStore`
Thread-safe in-memory storage for published events with the following features:
- Automatic offset assignment per partition (starting from 0)
- Organized by topic and partition via `TopicPartitionKey`
- Multiple retrieval methods with filtering capabilities
- Record counting and topic listing
- Transaction support (pending, committed, aborted states)
- Isolation-level-aware retrieval (READ_UNCOMMITTED / READ_COMMITTED)
- Log start offset and last-stable-offset (LSO) tracking
- Record deletion (advancing log start offset) and topic data purging

Uses a nested `PartitionStore` inner class per topic-partition, each maintaining:
- A synchronized list of `StoredRecord` objects
- An atomic offset counter (`AtomicLong nextOffset`)
- A log start offset (`AtomicLong logStartOffset`)
- A transaction state map per offset

### `TransactionState`
An enum representing the commit state of a transactionally-produced record:
- `PENDING` - written but transaction not yet finalized; visible only to READ_UNCOMMITTED consumers
- `COMMITTED` - transaction committed; visible at all isolation levels
- `ABORTED` - transaction aborted; never returned to consumers

Non-transactional records carry no state and are always visible.

### `TopicStore`
Metadata registry for topic definitions. Manages:
- `TopicCreationConfig` - configuration at creation time (partitions, replication factor, compression, cleanup policy, retention)
- `TopicDefinition` - full topic metadata including a stable UUID
- Topic creation, deletion, and partition count updates

### `CleanupPolicy`
An enum for log cleanup strategies:
- `DELETE` - time/size-based retention (default)
- `COMPACT` - log compaction (last record per key)
- `COMPACT_DELETE` - both compaction and time/size retention

### `AclStore`
Store for Access Control List bindings, backed by a concurrent set of `AclBinding` records.

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

// Get all topics that have received records
List<String> getKnownTopics()

// Get all topics registered via createTopic or admin API (even if no records published)
List<String> getRegisteredTopics()
```

### Maintenance

```java
// Clear all stored records (useful for test cleanup)
void clearRecords()
```

## EventStore Internal Methods

These methods are used by the protocol handlers and are not exposed on `KafkaesqueServer`:

### Isolation-Level-Aware Retrieval

```java
// Get records filtered by isolation level (0 = READ_UNCOMMITTED, 1 = READ_COMMITTED)
List<StoredRecord> getRecords(String topic, int partition, byte isolationLevel)

// Get last stable offset (first pending offset, or high watermark if no pending transactions)
long getLastStableOffset(String topic, int partition)
```

### Transactional Storage

```java
// Store a record as part of a pending transaction
long storePendingRecord(String transactionalId, RecordData data)

// Commit all pending records for a transaction
void commitTransaction(String transactionalId)

// Abort all pending records for a transaction
void abortTransaction(String transactionalId)

// Query transaction state of a specific record
TransactionState getTransactionState(String topic, int partition, long offset)
```

### Record Deletion

```java
// Advance the log start offset, effectively deleting earlier records
long deleteRecordsBefore(String topic, int partition, long beforeOffset)

// Purge all data for a topic
void deleteTopicData(String topic)
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
- Offsets are assigned sequentially and atomically via `AtomicLong`
- Different partitions have independent offset sequences

### Thread Safety
- The `EventStore` is fully thread-safe
- Uses `ConcurrentHashMap` for partition storage with `computeIfAbsent` for lazy initialization
- Synchronized access to record lists
- Returns immutable copies from all retrieval methods

### Transaction Support
- Records produced within a transaction are initially stored as `PENDING`
- The `TransactionCoordinator` calls `commitTransaction` or `abortTransaction` to finalize
- `READ_COMMITTED` consumers only see records up to the last-stable-offset (LSO)
- `READ_UNCOMMITTED` consumers see all records regardless of transaction state
- Aborted records are filtered out at all isolation levels

### Record Storage
- Records are stored in the order they are received
- All metadata from the Kafka protocol is preserved
- Keys and values are extracted from the Kafka record batches as strings

### Record Deletion
- `deleteRecordsBefore` advances the log start offset; records below it are excluded from retrieval
- `deleteTopicData` purges all partition stores for a topic

## Listeners

Kafkaesque supports registering listeners that receive callbacks when server events occur.
Listeners are registered on `KafkaesqueServer` and dispatched asynchronously via a
`LinkedBlockingQueue` consumed by a dedicated daemon thread, so listener execution never
blocks the NIO event loop.

### Listener Types

| Interface | Fires when |
|---|---|
| `RecordPublishedListener` | A record is stored (transactional-pending or non-transactional) |
| `TopicCreatedListener` | A new topic is registered (not on duplicate creation) |
| `TransactionCompletedListener` | A transaction is committed or aborted |

### Architecture

- `ListenerNotification` is a sealed interface with record subtypes (`RecordPublished`,
  `TopicCreated`, `TransactionCompleted`, `Shutdown`) representing queued notifications.
- `ListenerRegistry` holds registered listeners in thread-safe lists and a
  `LinkedBlockingQueue` of notifications. A daemon thread consumes the queue and dispatches
  to listeners. `close()` sends a poison pill and waits for the thread to drain and stop.
- `EventStore` enqueues `RecordPublished` notifications in `storeRecord()` and
  `storePendingRecord()`, and `TransactionCompleted` notifications in `commitTransaction()`
  and `abortTransaction()`.
- `TopicStore` enqueues `TopicCreated` notifications in `createTopic()` when a topic is
  genuinely new.
- `KafkaProtocolHandler` creates a shared `ListenerRegistry` and passes it to both stores.
  Its `close()` method shuts down the registry's consumer thread.
- `KafkaesqueServer` exposes `addRecordPublishedListener()`, `addTopicCreatedListener()`, and
  `addTransactionCompletedListener()` convenience methods.

See `LISTENERS.md` for usage examples.

## Testing

### Unit Tests
- `EventStoreTest` - Comprehensive tests for the EventStore class
  - 32 test cases covering all functionality
  - Tests for offset assignment, filtering, counting, transactions, isolation levels, listeners, and edge cases
- `ListenerRegistryTest` - Tests for the listener registry
  - 12 test cases covering registration, notification, multiple listeners, exception handling

### Event Retrieval Tests
- `KafkaesqueServerEventRetrievalTest` - Tests with real Kafka producer
  - 11 test cases located in `kafkaesque-core/src/test/`
  - Tests publishing and retrieval workflows
  - Tests filtering and counting
  - Tests with multiple topics and partitions
