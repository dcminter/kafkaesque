package eu.kafkaesque.core.storage;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

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
@Getter
@RequiredArgsConstructor
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
    private final List<RecordHeader> headers;
}
