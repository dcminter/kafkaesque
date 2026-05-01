package eu.kafkaesque.core.storage;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.time.Instant;
import java.util.List;

import static java.util.List.copyOf;
import static java.util.List.of;

/**
 * Represents a single record that was published to Kafkaesque.
 *
 * <p>This class captures all relevant metadata about a published event including
 * its key, value, timestamp, headers, and position within the topic partition.</p>
 */
@EqualsAndHashCode
@Getter
public final class StoredRecord {

    /** The topic name this record was published to. */
    private final String topic;

    /** The partition index within the topic. */
    private final int partition;

    /** The offset assigned to this record within the partition. */
    private final long offset;

    /** The timestamp when the record was published (epoch milliseconds). */
    private final long timestamp;

    /** The record headers (never null; empty if none were set). */
    private final List<RecordHeader> headers;

    /** The record key as a string (nullable). */
    private final String key;

    /** The record value as a string (nullable). */
    private final String value;

    /**
     * Creates a new stored record, defensively copying the headers list and normalising null to empty.
     *
     * @param topic     the topic name this record was published to
     * @param partition the partition index within the topic
     * @param offset    the offset assigned to this record within the partition
     * @param timestamp the timestamp when the record was published (epoch milliseconds)
     * @param headers   the record headers (never null; empty if none were set)
     * @param key       the record key as a string (nullable)
     * @param value     the record value as a string (nullable)
     */
    @Builder
    public StoredRecord(
            final String topic,
            final int partition,
            final long offset,
            final long timestamp,
            final List<RecordHeader> headers,
            final String key,
            final String value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.headers = (headers == null) ? of() : copyOf(headers);
        this.key = key;
        this.value = value;
    }

    /**
     * Gets the timestamp as an Instant for easier time-based operations.
     *
     * @return the timestamp as an Instant
     */
    public Instant timestampAsInstant() {
        return Instant.ofEpochMilli(timestamp);
    }

    /**
     * Returns a human-readable string representation of this record.
     *
     * @return string representation including all fields
     */
    @Override
    public String toString() {
        return String.format("StoredRecord[topic=%s, partition=%d, offset=%d, timestamp=%s, headers=%d, key=%s, value=%s]",
            topic, partition, offset, timestampAsInstant(), key, value, headers.size());
    }
}
