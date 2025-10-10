package eu.kafkaesque.core;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for event retrieval methods on {@link KafkaesqueServer}.
 */
@Disabled
class KafkaesqueServerEventRetrievalTest {

    private KafkaesqueServer server;
    private KafkaProducer<String, String> producer;

    @BeforeEach
    void setUp() throws IOException {
        server = new KafkaesqueServer("localhost", 0);
        server.start();

        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

        producer = new KafkaProducer<>(props);
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            try {
                producer.close(java.time.Duration.ofSeconds(5));
            } catch (final Exception e) {
                // Ignore close exceptions
            }
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void shouldRetrievePublishedRecords() throws ExecutionException, InterruptedException {
        final var topic = "test-topic";

        // Publish some records
        producer.send(new ProducerRecord<>(topic, "key1", "value1")).get();
        producer.send(new ProducerRecord<>(topic, "key2", "value2")).get();
        producer.send(new ProducerRecord<>(topic, "key3", "value3")).get();
        producer.flush();

        // Retrieve records
        final var records = server.getRecordsByTopic(topic);

        assertEquals(3, records.size(), "Should have 3 records");
        assertEquals("value1", records.get(0).value());
        assertEquals("value2", records.get(1).value());
        assertEquals("value3", records.get(2).value());
    }

    @Test
    void shouldRetrieveRecordsByKey() throws ExecutionException, InterruptedException {
        final var topic = "test-topic";

        // Publish records with different keys
        producer.send(new ProducerRecord<>(topic, "user-123", "login")).get();
        producer.send(new ProducerRecord<>(topic, "user-456", "logout")).get();
        producer.send(new ProducerRecord<>(topic, "user-123", "purchase")).get();
        producer.flush();

        // Retrieve records by key
        final var user123Records = server.getRecordsByTopicAndKey(topic, "user-123");
        final var user456Records = server.getRecordsByTopicAndKey(topic, "user-456");

        assertEquals(2, user123Records.size(), "Should have 2 records for user-123");
        assertEquals(1, user456Records.size(), "Should have 1 record for user-456");
        assertEquals("login", user123Records.get(0).value());
        assertEquals("purchase", user123Records.get(1).value());
        assertEquals("logout", user456Records.get(0).value());
    }

    @Test
    void shouldRetrieveRecordsFromSpecificPartition() throws ExecutionException, InterruptedException {
        final var topic = "test-topic";
        final var partition = 0;

        // Publish to specific partition
        producer.send(new ProducerRecord<>(topic, partition, "key1", "value1")).get();
        producer.send(new ProducerRecord<>(topic, partition, "key2", "value2")).get();
        producer.flush();

        // Retrieve from specific partition
        final var records = server.getRecords(topic, partition);

        assertEquals(2, records.size(), "Should have 2 records in partition 0");
        assertEquals(partition, records.get(0).partition());
        assertEquals(partition, records.get(1).partition());
    }

    @Test
    void shouldGetRecordCounts() throws ExecutionException, InterruptedException {
        final var topic1 = "topic1";
        final var topic2 = "topic2";

        // Publish to different topics
        producer.send(new ProducerRecord<>(topic1, "key1", "value1")).get();
        producer.send(new ProducerRecord<>(topic1, "key2", "value2")).get();
        producer.send(new ProducerRecord<>(topic2, "key3", "value3")).get();
        producer.flush();

        // Check counts
        assertEquals(3L, server.getTotalRecordCount(), "Should have 3 total records");
        assertEquals(2L, server.getRecordCount(topic1), "Topic1 should have 2 records");
        assertEquals(1L, server.getRecordCount(topic2), "Topic2 should have 1 record");
    }

    @Test
    void shouldGetKnownTopics() throws ExecutionException, InterruptedException {
        // Publish to multiple topics
        producer.send(new ProducerRecord<>("topic-alpha", "key", "value")).get();
        producer.send(new ProducerRecord<>("topic-beta", "key", "value")).get();
        producer.flush();

        final var topics = server.getKnownTopics();

        assertEquals(2, topics.size(), "Should have 2 topics");
        assertTrue(topics.contains("topic-alpha"), "Should contain topic-alpha");
        assertTrue(topics.contains("topic-beta"), "Should contain topic-beta");
    }

    @Test
    void shouldFindRecordsWithPredicate() throws ExecutionException, InterruptedException {
        final var topic = "test-topic";

        // Publish records
        producer.send(new ProducerRecord<>(topic, "key1", "hello world")).get();
        producer.send(new ProducerRecord<>(topic, "key2", "goodbye world")).get();
        producer.send(new ProducerRecord<>(topic, "key3", "hello universe")).get();
        producer.flush();

        // Find records containing "hello"
        final var helloRecords = server.findRecords(record ->
            record.value() != null && record.value().contains("hello"));

        assertEquals(2, helloRecords.size(), "Should find 2 records containing 'hello'");
    }

    @Test
    void shouldClearRecords() throws ExecutionException, InterruptedException {
        final var topic = "test-topic";

        // Publish some records
        producer.send(new ProducerRecord<>(topic, "key1", "value1")).get();
        producer.send(new ProducerRecord<>(topic, "key2", "value2")).get();
        producer.flush();

        assertEquals(2L, server.getTotalRecordCount(), "Should have 2 records before clear");

        // Clear records
        server.clearRecords();

        assertEquals(0L, server.getTotalRecordCount(), "Should have 0 records after clear");
        assertTrue(server.getAllRecords().isEmpty(), "Should return empty list after clear");
    }

    @Test
    void shouldPreserveRecordMetadata() throws ExecutionException, InterruptedException {
        final var topic = "test-topic";
        final var partition = 0;
        final var key = "test-key";
        final var value = "test-value";

        // Publish a record
        producer.send(new ProducerRecord<>(topic, partition, key, value)).get();
        producer.flush();

        // Retrieve and verify
        final var records = server.getRecords(topic, partition);
        assertEquals(1, records.size(), "Should have 1 record");

        final var record = records.get(0);
        assertEquals(topic, record.topic(), "Topic should match");
        assertEquals(partition, record.partition(), "Partition should match");
        assertEquals(0L, record.offset(), "Should have offset 0");
        assertEquals(key, record.key(), "Key should match");
        assertEquals(value, record.value(), "Value should match");
        assertTrue(record.timestamp() > 0, "Timestamp should be positive");
    }

    @Test
    void shouldHandleNullKeys() throws ExecutionException, InterruptedException {
        final var topic = "test-topic";

        // Publish records with null keys
        producer.send(new ProducerRecord<>(topic, null, "value1")).get();
        producer.send(new ProducerRecord<>(topic, null, "value2")).get();
        producer.flush();

        // Retrieve records with null key
        final var nullKeyRecords = server.getRecordsByTopicAndKey(topic, null);

        assertEquals(2, nullKeyRecords.size(), "Should find 2 records with null key");
        assertNull(nullKeyRecords.get(0).key(), "Key should be null");
        assertNull(nullKeyRecords.get(1).key(), "Key should be null");
    }

    @Test
    void shouldReturnEmptyListForNonExistentTopic() {
        final var records = server.getRecordsByTopic("non-existent-topic");

        assertNotNull(records, "Should return non-null list");
        assertTrue(records.isEmpty(), "Should return empty list for non-existent topic");
    }

    @Test
    void shouldHandleMultiplePartitions() throws ExecutionException, InterruptedException {
        final var topic = "test-topic";

        // Publish to different partitions
        producer.send(new ProducerRecord<>(topic, 0, "key1", "value1")).get();
        producer.send(new ProducerRecord<>(topic, 0, "key2", "value2")).get();
        producer.send(new ProducerRecord<>(topic, 1, "key3", "value3")).get();
        producer.flush();

        // Verify partition-specific retrieval
        assertEquals(2, server.getRecords(topic, 0).size(), "Partition 0 should have 2 records");
        assertEquals(1, server.getRecords(topic, 1).size(), "Partition 1 should have 1 record");

        // Verify topic-wide retrieval
        assertEquals(3, server.getRecordsByTopic(topic).size(), "Topic should have 3 records total");
    }
}
