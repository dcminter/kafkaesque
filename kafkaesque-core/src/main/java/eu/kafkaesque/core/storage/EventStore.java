package eu.kafkaesque.core.storage;

import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
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
 * <p>Transactional records are stored immediately but marked as {@link TransactionState#PENDING}
 * until the owning transaction is committed or aborted. Commit transitions pending records to
 * {@link TransactionState#COMMITTED} (making them visible to all consumers); abort transitions
 * them to {@link TransactionState#ABORTED} (hiding them from all consumers).</p>
 *
 * <p>The store is thread-safe and can be accessed concurrently by multiple threads.</p>
 */
@Slf4j
public final class EventStore {

    /**
     * Identifies a topic partition as a store key.
     */
    private record TopicPartitionKey(String topic, int partition) {}

    /**
     * Records a single pending record's location for later commit/abort.
     */
    private record PartitionOffset(String topic, int partition, long offset) {}

    /**
     * Represents a partition's storage with its records and offset counter.
     */
    private static final class PartitionStore {
        private final List<StoredRecord> records = Collections.synchronizedList(new ArrayList<>());
        private final AtomicLong nextOffset = new AtomicLong(0L);
        private final Map<Long, TransactionState> transactionStates = new ConcurrentHashMap<>();

        /**
         * Atomically assigns an offset to a new record, stores it, and returns the offset.
         *
         * <p>The record is constructed inside this method so that the offset baked into the
         * {@link StoredRecord} is always the value returned by the atomic increment,
         * eliminating the TOCTOU race that would arise if the caller pre-built the record
         * using a separate {@link #getNextOffset()} peek.</p>
         *
         * @param data the record data to store
         * @return the offset assigned to the stored record
         */
        long store(final RecordData data) {
            final var offset = nextOffset.getAndIncrement();
            records.add(new StoredRecord(
                data.topic(), data.partition(), offset, data.timestamp(),
                    data.headers(), data.key(), data.value()));
            return offset;
        }

        /**
         * Marks the given offset as belonging to a pending transaction.
         *
         * @param offset the record offset
         */
        void markPending(final long offset) {
            transactionStates.put(offset, TransactionState.PENDING);
        }

        /**
         * Updates the transaction state for the given offset.
         *
         * @param offset the record offset
         * @param state  the new state
         */
        void updateTransactionState(final long offset, final TransactionState state) {
            transactionStates.put(offset, state);
        }

        /**
         * Returns the transaction state for the given offset, or {@code null} for non-transactional records.
         *
         * @param offset the record offset
         * @return the transaction state, or {@code null}
         */
        TransactionState getTransactionState(final long offset) {
            return transactionStates.get(offset);
        }

        /**
         * Returns the lowest offset that currently has {@link TransactionState#PENDING} state.
         *
         * @return the first pending offset, or empty if none
         */
        OptionalLong getFirstPendingOffset() {
            return transactionStates.entrySet().stream()
                .filter(e -> e.getValue() == TransactionState.PENDING)
                .mapToLong(Map.Entry::getKey)
                .min();
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

    private final Map<TopicPartitionKey, PartitionStore> partitions = new ConcurrentHashMap<>();

    /**
     * Pending records per transactional ID, used to commit or abort in bulk.
     * Access is guarded by the list's own synchronization.
     */
    private final Map<String, List<PartitionOffset>> pendingByTransaction = new ConcurrentHashMap<>();

    /**
     * Stores a published record with no headers and assigns it an offset.
     *
     * <p>Convenience overload for callers that do not need to supply headers;
     * delegates to {@link #storeRecord(RecordData)} with an empty header list.</p>
     *
     * @param topic     the topic name
     * @param partition the partition index
     * @param timestamp the record timestamp (epoch milliseconds)
     * @param key       the record key (nullable)
     * @param value     the record value (nullable)
     * @return the assigned offset
     */
    public long storeRecord(
            final String topic,
            final int partition,
            final long timestamp,
            final String key,
            final String value) {
        return storeRecord(new RecordData(topic, partition, timestamp, key, value, List.of()));
    }

    /**
     * Stores a published record and assigns it an offset.
     *
     * <p>The offset is assigned atomically per partition, starting from 0.</p>
     *
     * @param data the record data to store
     * @return the assigned offset
     */
    public long storeRecord(final RecordData data) {
        final var partitionKey = new TopicPartitionKey(data.topic(), data.partition());
        final var partitionStore = partitions.computeIfAbsent(partitionKey, k -> new PartitionStore());

        final var offset = partitionStore.store(data);

        log.debug("Stored record on topic={} partition={} at offset={}", data.topic(), data.partition(), offset);

        return offset;
    }

    /**
     * Stores a record as part of a pending transaction and assigns it an offset.
     *
     * <p>The record is immediately assigned an offset and written to the partition log,
     * but is marked {@link TransactionState#PENDING} until the transaction is committed
     * or aborted via {@link #commitTransaction(String)} or {@link #abortTransaction(String)}.</p>
     *
     * @param transactionalId the transactional producer ID owning this record
     * @param data            the record data to store
     * @return the assigned offset
     */
    public long storePendingRecord(final String transactionalId, final RecordData data) {
        final var partitionKey = new TopicPartitionKey(data.topic(), data.partition());
        final var partitionStore = partitions.computeIfAbsent(partitionKey, k -> new PartitionStore());

        final var offset = partitionStore.store(data);
        partitionStore.markPending(offset);

        pendingByTransaction
            .computeIfAbsent(transactionalId, k -> Collections.synchronizedList(new ArrayList<>()))
            .add(new PartitionOffset(data.topic(), data.partition(), offset));

        log.debug("Stored pending record for transaction {} on topic={} partition={} at offset={}",
            transactionalId, data.topic(), data.partition(), offset);

        return offset;
    }

    /**
     * Commits all pending records belonging to the given transactional ID.
     *
     * <p>All records previously stored via {@link #storePendingRecord} with this
     * transactional ID are transitioned to {@link TransactionState#COMMITTED}, making
     * them visible to {@code READ_COMMITTED} consumers.</p>
     *
     * @param transactionalId the transactional producer ID to commit
     */
    public void commitTransaction(final String transactionalId) {
        final var pending = pendingByTransaction.remove(transactionalId);
        if (pending != null) {
            synchronized (pending) {
                for (final var po : pending) {
                    final var ps = partitions.get(new TopicPartitionKey(po.topic(), po.partition()));
                    if (ps != null) {
                        ps.updateTransactionState(po.offset(), TransactionState.COMMITTED);
                    }
                }
            }
            log.debug("Committed transaction {} ({} record(s))", transactionalId, pending.size());
        }
    }

    /**
     * Aborts all pending records belonging to the given transactional ID.
     *
     * <p>All records previously stored via {@link #storePendingRecord} with this
     * transactional ID are transitioned to {@link TransactionState#ABORTED}, hiding
     * them from all consumers regardless of isolation level.</p>
     *
     * @param transactionalId the transactional producer ID to abort
     */
    public void abortTransaction(final String transactionalId) {
        final var pending = pendingByTransaction.remove(transactionalId);
        if (pending != null) {
            synchronized (pending) {
                for (final var po : pending) {
                    final var ps = partitions.get(new TopicPartitionKey(po.topic(), po.partition()));
                    if (ps != null) {
                        ps.updateTransactionState(po.offset(), TransactionState.ABORTED);
                    }
                }
            }
            log.debug("Aborted transaction {} ({} record(s))", transactionalId, pending.size());
        }
    }

    /**
     * Returns the transaction state of a specific record, or {@code null} for non-transactional records.
     *
     * @param topic     the topic name
     * @param partition the partition index
     * @param offset    the record offset
     * @return the transaction state, or {@code null} if the record is non-transactional
     */
    public TransactionState getTransactionState(final String topic, final int partition, final long offset) {
        final var ps = partitions.get(new TopicPartitionKey(topic, partition));
        return ps != null ? ps.getTransactionState(offset) : null;
    }

    /**
     * Returns the last stable offset (LSO) for a partition.
     *
     * <p>The LSO is the offset of the first {@link TransactionState#PENDING} record in the
     * partition, or the high-watermark (next offset to be assigned) if there are no open
     * transactions. {@code READ_COMMITTED} consumers may only safely read records below the LSO.</p>
     *
     * @param topic     the topic name
     * @param partition the partition index
     * @return the last stable offset
     */
    public long getLastStableOffset(final String topic, final int partition) {
        final var ps = partitions.get(new TopicPartitionKey(topic, partition));
        if (ps == null) {
            return 0L;
        }
        return ps.getFirstPendingOffset().orElse(ps.getNextOffset());
    }

    /**
     * Retrieves all records for a specific topic and partition.
     *
     * @param topic     the topic name
     * @param partition the partition index
     * @return unmodifiable list of records (empty if partition doesn't exist)
     */
    public List<StoredRecord> getRecords(final String topic, final int partition) {
        final var partitionKey = new TopicPartitionKey(topic, partition);
        final var partitionStore = partitions.get(partitionKey);
        return partitionStore != null ? partitionStore.getRecords() : List.of();
    }

    /**
     * Retrieves records for a specific topic and partition, filtered by isolation level.
     *
     * <p>Isolation level {@code 0} ({@code READ_UNCOMMITTED}) returns all records regardless
     * of transaction state (including {@link TransactionState#ABORTED} ones), matching real
     * Kafka's contract that READ_UNCOMMITTED consumers see all messages. Isolation level
     * {@code 1} ({@code READ_COMMITTED}) returns only non-transactional records and records
     * with {@link TransactionState#COMMITTED} state.</p>
     *
     * @param topic          the topic name
     * @param partition      the partition index
     * @param isolationLevel {@code 0} for READ_UNCOMMITTED, {@code 1} for READ_COMMITTED
     * @return filtered list of records
     */
    public List<StoredRecord> getRecords(final String topic, final int partition, final byte isolationLevel) {
        final var partitionKey = new TopicPartitionKey(topic, partition);
        final var ps = partitions.get(partitionKey);
        if (ps == null) {
            return List.of();
        }
        return ps.getRecords().stream()
            .filter(r -> passesIsolationFilter(ps, r.offset(), isolationLevel))
            .toList();
    }

    /**
     * Determines whether a record at the given offset passes the isolation-level filter.
     *
     * <p>For {@code READ_UNCOMMITTED} (level {@code 0}) all records are returned:
     * non-transactional, pending, committed, and aborted alike. This matches real Kafka's
     * contract: "consumer.poll() will return all messages, even transactional messages which
     * have been aborted."</p>
     *
     * <p>For {@code READ_COMMITTED} (level {@code 1}) only non-transactional records and
     * {@link TransactionState#COMMITTED} records pass. {@link TransactionState#PENDING}
     * records are additionally excluded by the last-stable-offset check in the caller.</p>
     *
     * @param ps             the partition store
     * @param offset         the record offset
     * @param isolationLevel {@code 0} for READ_UNCOMMITTED, {@code 1} for READ_COMMITTED
     * @return {@code true} if the record should be included in the result
     */
    private static boolean passesIsolationFilter(
            final PartitionStore ps, final long offset, final byte isolationLevel) {
        if (isolationLevel != 1) {
            // READ_UNCOMMITTED: all records are visible regardless of transaction state
            return true;
        }
        final var state = ps.getTransactionState(offset);
        // READ_COMMITTED: hide aborted and pending records
        return state != TransactionState.ABORTED && state != TransactionState.PENDING;
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
     * @param key   the key to match (nullable)
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
     * <p>This equals the high-watermark (the next offset to be assigned) since offsets
     * start at zero and are contiguous.</p>
     *
     * @param topic     the topic name
     * @param partition the partition index
     * @return the record count for the partition
     */
    public long getRecordCount(final String topic, final int partition) {
        return getRecords(topic, partition).size();
    }

    /**
     * Clears all stored records and pending transaction tracking.
     *
     * <p>This is useful for testing scenarios where you want to reset state.</p>
     */
    public void clear() {
        partitions.clear();
        pendingByTransaction.clear();
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
