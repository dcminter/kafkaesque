package eu.kafkaesque.core.storage;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.kafka.common.header.Header;

import java.util.List;

/**
 * Captures the data fields of a single record to be stored in the event store.
 *
 * <p>Used as a context object to avoid long parameter lists on {@link EventStore} storage methods.
 * Both {@link EventStore#storeRecord(RecordData)} and
 * {@link EventStore#storePendingRecord(String, RecordData)} accept this record.</p>
 */
@EqualsAndHashCode
@ToString
public final class RecordData {

    /** The topic name. */
    private final String topic;

    /** The partition index. */
    private final int partition;

    /** The record timestamp (epoch milliseconds). */
    private final long timestamp;

    /** The record key (nullable). */
    private final String key;

    /** The record value (nullable). */
    private final String value;

    /** The record headers (may be empty, never null). */
    private final List<Header> headers;

    /**
     * Creates a new record data instance.
     *
     * @param topic     the topic name
     * @param partition the partition index
     * @param timestamp the record timestamp (epoch milliseconds)
     * @param key       the record key (nullable)
     * @param value     the record value (nullable)
     * @param headers   the record headers (may be empty, never null)
     */
    public RecordData(
            final String topic,
            final int partition,
            final long timestamp,
            final String key,
            final String value,
            final List<Header> headers) {
        this.topic = topic;
        this.partition = partition;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    /**
     * Returns the topic name.
     *
     * @return the topic name
     */
    public String topic() {
        return topic;
    }

    /**
     * Returns the partition index.
     *
     * @return the partition index
     */
    public int partition() {
        return partition;
    }

    /**
     * Returns the record timestamp (epoch milliseconds).
     *
     * @return the timestamp
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Returns the record key (nullable).
     *
     * @return the record key
     */
    public String key() {
        return key;
    }

    /**
     * Returns the record value (nullable).
     *
     * @return the record value
     */
    public String value() {
        return value;
    }

    /**
     * Returns the record headers (may be empty, never null).
     *
     * @return the record headers
     */
    public List<Header> headers() {
        return headers;
    }
}
