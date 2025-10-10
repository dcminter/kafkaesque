package eu.kafkaesque.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EventStore}.
 */
class EventStoreTest {

    private EventStore eventStore;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
    }

    @Test
    void storeRecord_shouldAssignSequentialOffsets() {
        final var topic = "test-topic";
        final var partition = 0;
        final var timestamp = System.currentTimeMillis();

        final var offset1 = eventStore.storeRecord(topic, partition, timestamp, "key1", "value1");
        final var offset2 = eventStore.storeRecord(topic, partition, timestamp, "key2", "value2");
        final var offset3 = eventStore.storeRecord(topic, partition, timestamp, "key3", "value3");

        assertEquals(0L, offset1, "First offset should be 0");
        assertEquals(1L, offset2, "Second offset should be 1");
        assertEquals(2L, offset3, "Third offset should be 2");
    }

    @Test
    void storeRecord_shouldHandleMultiplePartitions() {
        final var topic = "test-topic";
        final var timestamp = System.currentTimeMillis();

        final var offset1 = eventStore.storeRecord(topic, 0, timestamp, "key1", "value1");
        final var offset2 = eventStore.storeRecord(topic, 1, timestamp, "key2", "value2");
        final var offset3 = eventStore.storeRecord(topic, 0, timestamp, "key3", "value3");

        assertEquals(0L, offset1, "First offset in partition 0 should be 0");
        assertEquals(0L, offset2, "First offset in partition 1 should be 0");
        assertEquals(1L, offset3, "Second offset in partition 0 should be 1");
    }

    @Test
    void getRecords_shouldReturnRecordsForSpecificPartition() {
        final var topic = "test-topic";
        final var timestamp = System.currentTimeMillis();

        eventStore.storeRecord(topic, 0, timestamp, "key1", "value1");
        eventStore.storeRecord(topic, 1, timestamp, "key2", "value2");
        eventStore.storeRecord(topic, 0, timestamp, "key3", "value3");

        final var partition0Records = eventStore.getRecords(topic, 0);
        final var partition1Records = eventStore.getRecords(topic, 1);

        assertEquals(2, partition0Records.size(), "Partition 0 should have 2 records");
        assertEquals(1, partition1Records.size(), "Partition 1 should have 1 record");
        assertEquals("value1", partition0Records.get(0).value());
        assertEquals("value3", partition0Records.get(1).value());
        assertEquals("value2", partition1Records.get(0).value());
    }

    @Test
    void getRecords_shouldReturnEmptyListForNonExistentPartition() {
        final var records = eventStore.getRecords("non-existent-topic", 0);

        assertNotNull(records, "Should return non-null list");
        assertTrue(records.isEmpty(), "Should return empty list for non-existent partition");
    }

    @Test
    void getRecordsByTopic_shouldReturnAllRecordsForTopic() {
        final var topic1 = "topic1";
        final var topic2 = "topic2";
        final var timestamp = System.currentTimeMillis();

        eventStore.storeRecord(topic1, 0, timestamp, "key1", "value1");
        eventStore.storeRecord(topic1, 1, timestamp, "key2", "value2");
        eventStore.storeRecord(topic2, 0, timestamp, "key3", "value3");

        final var topic1Records = eventStore.getRecordsByTopic(topic1);
        final var topic2Records = eventStore.getRecordsByTopic(topic2);

        assertEquals(2, topic1Records.size(), "Topic1 should have 2 records");
        assertEquals(1, topic2Records.size(), "Topic2 should have 1 record");
    }

    @Test
    void getRecordsByTopicAndKey_shouldFilterByKey() {
        final var topic = "test-topic";
        final var timestamp = System.currentTimeMillis();

        eventStore.storeRecord(topic, 0, timestamp, "keyA", "value1");
        eventStore.storeRecord(topic, 0, timestamp, "keyB", "value2");
        eventStore.storeRecord(topic, 0, timestamp, "keyA", "value3");
        eventStore.storeRecord(topic, 0, timestamp, null, "value4");

        final var keyARecords = eventStore.getRecordsByTopicAndKey(topic, "keyA");
        final var keyBRecords = eventStore.getRecordsByTopicAndKey(topic, "keyB");
        final var nullKeyRecords = eventStore.getRecordsByTopicAndKey(topic, null);

        assertEquals(2, keyARecords.size(), "Should find 2 records with keyA");
        assertEquals(1, keyBRecords.size(), "Should find 1 record with keyB");
        assertEquals(1, nullKeyRecords.size(), "Should find 1 record with null key");
        assertEquals("value1", keyARecords.get(0).value());
        assertEquals("value3", keyARecords.get(1).value());
    }

    @Test
    void getAllRecords_shouldReturnAllRecords() {
        final var timestamp = System.currentTimeMillis();

        eventStore.storeRecord("topic1", 0, timestamp, "key1", "value1");
        eventStore.storeRecord("topic1", 1, timestamp, "key2", "value2");
        eventStore.storeRecord("topic2", 0, timestamp, "key3", "value3");

        final var allRecords = eventStore.getAllRecords();

        assertEquals(3, allRecords.size(), "Should return all 3 records");
    }

    @Test
    void findRecords_shouldFilterByPredicate() {
        final var timestamp = System.currentTimeMillis();

        eventStore.storeRecord("topic1", 0, timestamp, "key1", "value1");
        eventStore.storeRecord("topic1", 0, timestamp, "key2", "value2");
        eventStore.storeRecord("topic2", 0, timestamp, "key3", "value3");

        final var topic1Records = eventStore.findRecords(record -> "topic1".equals(record.topic()));
        final var keyContains2 = eventStore.findRecords(record -> record.key() != null && record.key().contains("2"));

        assertEquals(2, topic1Records.size(), "Should find 2 records from topic1");
        assertEquals(1, keyContains2.size(), "Should find 1 record with '2' in key");
    }

    @Test
    void getTotalRecordCount_shouldReturnCorrectCount() {
        final var timestamp = System.currentTimeMillis();

        assertEquals(0L, eventStore.getTotalRecordCount(), "Should start with 0 records");

        eventStore.storeRecord("topic1", 0, timestamp, "key1", "value1");
        eventStore.storeRecord("topic1", 1, timestamp, "key2", "value2");
        eventStore.storeRecord("topic2", 0, timestamp, "key3", "value3");

        assertEquals(3L, eventStore.getTotalRecordCount(), "Should have 3 records total");
    }

    @Test
    void getRecordCount_forTopic_shouldReturnCorrectCount() {
        final var timestamp = System.currentTimeMillis();

        eventStore.storeRecord("topic1", 0, timestamp, "key1", "value1");
        eventStore.storeRecord("topic1", 1, timestamp, "key2", "value2");
        eventStore.storeRecord("topic2", 0, timestamp, "key3", "value3");

        assertEquals(2L, eventStore.getRecordCount("topic1"), "Topic1 should have 2 records");
        assertEquals(1L, eventStore.getRecordCount("topic2"), "Topic2 should have 1 record");
        assertEquals(0L, eventStore.getRecordCount("non-existent"), "Non-existent topic should have 0 records");
    }

    @Test
    void getRecordCount_forTopicAndPartition_shouldReturnCorrectCount() {
        final var timestamp = System.currentTimeMillis();

        eventStore.storeRecord("topic1", 0, timestamp, "key1", "value1");
        eventStore.storeRecord("topic1", 0, timestamp, "key2", "value2");
        eventStore.storeRecord("topic1", 1, timestamp, "key3", "value3");

        assertEquals(2L, eventStore.getRecordCount("topic1", 0), "Topic1 partition 0 should have 2 records");
        assertEquals(1L, eventStore.getRecordCount("topic1", 1), "Topic1 partition 1 should have 1 record");
        assertEquals(0L, eventStore.getRecordCount("topic1", 2), "Topic1 partition 2 should have 0 records");
    }

    @Test
    void getTopics_shouldReturnAllTopics() {
        final var timestamp = System.currentTimeMillis();

        eventStore.storeRecord("zebra-topic", 0, timestamp, "key1", "value1");
        eventStore.storeRecord("alpha-topic", 0, timestamp, "key2", "value2");
        eventStore.storeRecord("zebra-topic", 1, timestamp, "key3", "value3");

        final var topics = eventStore.getTopics();

        assertEquals(2, topics.size(), "Should have 2 distinct topics");
        assertEquals(List.of("alpha-topic", "zebra-topic"), topics, "Topics should be sorted alphabetically");
    }

    @Test
    void clear_shouldRemoveAllRecords() {
        final var timestamp = System.currentTimeMillis();

        eventStore.storeRecord("topic1", 0, timestamp, "key1", "value1");
        eventStore.storeRecord("topic1", 1, timestamp, "key2", "value2");

        assertEquals(2L, eventStore.getTotalRecordCount(), "Should have 2 records before clear");

        eventStore.clear();

        assertEquals(0L, eventStore.getTotalRecordCount(), "Should have 0 records after clear");
        assertTrue(eventStore.getAllRecords().isEmpty(), "Should return empty list after clear");
        assertTrue(eventStore.getTopics().isEmpty(), "Should have no topics after clear");
    }

    @Test
    void storeRecord_shouldHandleNullKeyAndValue() {
        final var topic = "test-topic";
        final var partition = 0;
        final var timestamp = System.currentTimeMillis();

        final var offset = eventStore.storeRecord(topic, partition, timestamp, null, null);

        assertEquals(0L, offset, "Should assign offset 0");

        final var records = eventStore.getRecords(topic, partition);
        assertEquals(1, records.size(), "Should store 1 record");

        final var record = records.get(0);
        assertNull(record.key(), "Key should be null");
        assertNull(record.value(), "Value should be null");
    }

    @Test
    void storedRecord_shouldPreserveAllMetadata() {
        final var topic = "test-topic";
        final var partition = 5;
        final var timestamp = System.currentTimeMillis();
        final var key = "test-key";
        final var value = "test-value";

        eventStore.storeRecord(topic, partition, timestamp, key, value);

        final var records = eventStore.getRecords(topic, partition);
        assertEquals(1, records.size(), "Should have 1 record");

        final var record = records.get(0);
        assertEquals(topic, record.topic(), "Topic should match");
        assertEquals(partition, record.partition(), "Partition should match");
        assertEquals(0L, record.offset(), "Offset should be 0");
        assertEquals(timestamp, record.timestamp(), "Timestamp should match");
        assertEquals(key, record.key(), "Key should match");
        assertEquals(value, record.value(), "Value should match");
    }

    @Test
    void storedRecord_timestampAsInstant_shouldConvertCorrectly() {
        final var timestamp = 1704067200000L; // 2024-01-01 00:00:00 UTC
        final var expectedInstant = Instant.ofEpochMilli(timestamp);

        eventStore.storeRecord("topic", 0, timestamp, "key", "value");

        final var record = eventStore.getRecords("topic", 0).get(0);
        assertEquals(expectedInstant, record.timestampAsInstant(), "Instant conversion should be correct");
    }

    @Test
    void returnedLists_shouldBeUnmodifiable() {
        final var timestamp = System.currentTimeMillis();
        eventStore.storeRecord("topic", 0, timestamp, "key", "value");

        final var records = eventStore.getRecords("topic", 0);
        assertThrows(UnsupportedOperationException.class,
            () -> records.add(new StoredRecord("topic", 0, 0, timestamp, "key2", "value2")),
            "Returned list should be unmodifiable");

        final var allRecords = eventStore.getAllRecords();
        assertThrows(UnsupportedOperationException.class,
            () -> allRecords.clear(),
            "Returned list should be unmodifiable");

        final var topics = eventStore.getTopics();
        assertThrows(UnsupportedOperationException.class,
            () -> topics.add("new-topic"),
            "Returned list should be unmodifiable");
    }
}
