package eu.kafkaesque.core.storage;

import org.apache.kafka.common.header.Header;

import java.time.Instant;
import java.util.List;

/**
 * Represents a single record that was published to Kafkaesque.
 *
 * <p>This record captures all relevant metadata about a published event including
 * its key, value, timestamp, headers, and position within the topic partition.</p>
 *
 * @param topic     the topic name this record was published to
 * @param partition the partition index within the topic
 * @param offset    the offset assigned to this record within the partition
 * @param timestamp the timestamp when the record was published (epoch milliseconds)
 * @param headers   the record headers (never null; empty if none were set)
 * @param key       the record key as a string (nullable)
 * @param value     the record value as a string (nullable)
 */
public record StoredRecord(
        String topic,
        int partition,
        long offset,
        long timestamp,
        List<Header> headers, String key,
        String value
) {
    /**
     * Compact constructor that defensively copies the headers list and normalises null to empty.
     */
    public StoredRecord {
        headers = (headers == null) ? List.of() : List.copyOf(headers);
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
        return "StoredRecord[topic=%s, partition=%d, offset=%d, timestamp=%s, headers=%d, key=%s, value=%s]"
            .formatted(topic, partition, offset, timestampAsInstant(), key, value, headers.size());
    }
}
