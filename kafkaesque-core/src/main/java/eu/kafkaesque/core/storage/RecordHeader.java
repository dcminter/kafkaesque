package eu.kafkaesque.core.storage;

/**
 * Represents a single key-value header attached to a record.
 *
 * <p>This is Kafkaesque's own header type, decoupled from the Kafka client library.
 * It mirrors the shape of the Kafka {@code Header} interface so that header data
 * survives the produce-store-fetch round-trip without forcing users onto a specific
 * Kafka client version.</p>
 */
public interface RecordHeader {

    /**
     * Returns the header key.
     *
     * @return the key (never null)
     */
    String key();

    /**
     * Returns the header value as a byte array.
     *
     * @return the value (may be null)
     */
    byte[] value();
}
