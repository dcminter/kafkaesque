package eu.kafkaesque.core.storage;

import org.apache.kafka.common.header.Header;

import java.util.List;

/**
 * Captures the data fields of a single record to be stored in the event store.
 *
 * <p>Used as a context object to avoid long parameter lists on {@link EventStore} storage methods.
 * Both {@link EventStore#storeRecord(RecordData)} and
 * {@link EventStore#storePendingRecord(String, RecordData)} accept this record.</p>
 *
 * @param topic     the topic name
 * @param partition the partition index
 * @param timestamp the record timestamp (epoch milliseconds)
 * @param key       the record key (nullable)
 * @param value     the record value (nullable)
 * @param headers   the record headers (may be empty, never null)
 */
public record RecordData(
        String topic,
        int partition,
        long timestamp,
        String key,
        String value,
        List<Header> headers) {
}
