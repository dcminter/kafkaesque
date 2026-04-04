package eu.kafkaesque.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Thread-safe storage for events published to Kafkaesque.
 *
 * <p>This class maintains an in-memory store of all published records, organized by
 * topic and partition. Each partition maintains its own offset counter.</p>
 *
 * <p>The store is thread-safe and can be accessed concurrently by multiple threads.</p>
 */
@Slf4j
public final class EventStore {

    /**
     * Represents a partition's storage with its records and offset counter.
     */
    private static final class PartitionStore {
        private final List<StoredRecord> records = Collections.synchronizedList(new ArrayList<>());
        private final AtomicLong nextOffset = new AtomicLong(0L);

        /**
         * Stores a record and returns the assigned offset.
         *
         * @param record the record to store
         * @return the assigned offset
         */
        long store(final StoredRecord record) {
            final var offset = nextOffset.getAndIncrement();
            records.add(record);
            return offset;
        }

        /**
         * Gets all records in this partition.
         *
         * @return unmodifiable list of records
         */
        List<StoredRecord> getRecords() {
            synchronized (records) {
                return List.copyOf(records);
            }
        }

        /**
         * Gets the current next offset for this partition.
         *
         * @return the next offset that will be assigned
         */
        long getNextOffset() {
            return nextOffset.get();
        }
    }

    /**
     * Key for identifying a specific topic partition.
     */
    private record TopicPartitionKey(String topic, int partition) {}

    private final Map<TopicPartitionKey, PartitionStore> partitions = new ConcurrentHashMap<>();

    /**
     * Stores a published record with no headers and assigns it an offset.
     *
     * <p>Convenience overload for callers that do not need to supply headers;
     * delegates to {@link #storeRecord(String, int, long, String, String, List)}
     * with an empty header list.</p>
     *
     * @param topic the topic name
     * @param partition the partition index
     * @param timestamp the record timestamp (epoch milliseconds)
     * @param key the record key (nullable)
     * @param value the record value (nullable)
     * @return the assigned offset
     */
    public long storeRecord(
            final String topic,
            final int partition,
            final long timestamp,
            final String key,
            final String value) {
        return storeRecord(topic, partition, timestamp, key, value, List.of());
    }

    /**
     * Stores a published record and assigns it an offset.
     *
     * <p>The offset is assigned atomically per partition, starting from 0.</p>
     *
     * @param topic the topic name
     * @param partition the partition index
     * @param timestamp the record timestamp (epoch milliseconds)
     * @param key the record key (nullable)
     * @param value the record value (nullable)
     * @param headers the record headers (may be null or empty)
     * @return the assigned offset
     */
    public long storeRecord(
            final String topic,
            final int partition,
            final long timestamp,
            final String key,
            final String value,
            final List<Header> headers) {

        final var partitionKey = new TopicPartitionKey(topic, partition);
        final var partitionStore = partitions.computeIfAbsent(partitionKey, k -> new PartitionStore());

        final var offset = partitionStore.getNextOffset();
        final var record = new StoredRecord(topic, partition, offset, timestamp, key, value, headers);

        final var assignedOffset = partitionStore.store(record);

        log.debug("Stored record: {} at offset {}", record, assignedOffset);

        return offset;
    }

    /**
     * Retrieves all records for a specific topic and partition.
     *
     * @param topic the topic name
     * @param partition the partition index
     * @return unmodifiable list of records (empty if partition doesn't exist)
     */
    public List<StoredRecord> getRecords(final String topic, final int partition) {
        final var partitionKey = new TopicPartitionKey(topic, partition);
        final var partitionStore = partitions.get(partitionKey);
        return partitionStore != null ? partitionStore.getRecords() : List.of();
    }

    /**
     * Retrieves all records for a specific topic across all partitions.
     *
     * @param topic the topic name
     * @return unmodifiable list of records (empty if topic doesn't exist)
     */
    public List<StoredRecord> getRecordsByTopic(final String topic) {
        return partitions.entrySet().stream()
            .filter(entry -> entry.getKey().topic.equals(topic))
            .flatMap(entry -> entry.getValue().getRecords().stream())
            .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Retrieves all records across all topics and partitions.
     *
     * @return unmodifiable list of all records
     */
    public List<StoredRecord> getAllRecords() {
        return partitions.values().stream()
            .flatMap(store -> store.getRecords().stream())
            .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Retrieves records matching a specific predicate.
     *
     * @param predicate the filter predicate
     * @return unmodifiable list of matching records
     */
    public List<StoredRecord> findRecords(final Predicate<StoredRecord> predicate) {
        return getAllRecords().stream()
            .filter(predicate)
            .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Retrieves all records for a specific topic that match a key.
     *
     * @param topic the topic name
     * @param key the key to match (nullable)
     * @return unmodifiable list of matching records
     */
    public List<StoredRecord> getRecordsByTopicAndKey(final String topic, final String key) {
        return getRecordsByTopic(topic).stream()
            .filter(record -> {
                if (key == null) {
                    return record.key() == null;
                }
                return key.equals(record.key());
            })
            .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Gets the total number of records stored across all topics and partitions.
     *
     * @return the total record count
     */
    public long getTotalRecordCount() {
        return partitions.values().stream()
            .mapToLong(store -> store.getRecords().size())
            .sum();
    }

    /**
     * Gets the number of records stored for a specific topic.
     *
     * @param topic the topic name
     * @return the record count for the topic
     */
    public long getRecordCount(final String topic) {
        return partitions.entrySet().stream()
            .filter(entry -> entry.getKey().topic.equals(topic))
            .mapToLong(entry -> entry.getValue().getRecords().size())
            .sum();
    }

    /**
     * Gets the number of records stored for a specific topic and partition.
     *
     * @param topic the topic name
     * @param partition the partition index
     * @return the record count for the partition
     */
    public long getRecordCount(final String topic, final int partition) {
        return getRecords(topic, partition).size();
    }

    /**
     * Clears all stored records.
     *
     * <p>This is useful for testing scenarios where you want to reset state.</p>
     */
    public void clear() {
        partitions.clear();
        log.debug("Event store cleared");
    }

    /**
     * Gets all known topics.
     *
     * @return unmodifiable set of topic names
     */
    public List<String> getTopics() {
        return partitions.keySet().stream()
            .map(TopicPartitionKey::topic)
            .distinct()
            .sorted()
            .collect(Collectors.toUnmodifiableList());
    }
}
