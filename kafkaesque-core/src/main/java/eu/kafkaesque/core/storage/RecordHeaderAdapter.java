package eu.kafkaesque.core.storage;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Default implementation of {@link RecordHeader} backed by a key string and value byte array.
 *
 * <p>Used internally to convert wire-protocol headers into Kafkaesque's own header type.
 * Can also be used by test code that needs to construct headers programmatically.</p>
 */
@EqualsAndHashCode
@ToString
public final class RecordHeaderAdapter implements RecordHeader {

    /** The header key. */
    private final String key;

    /** The header value (may be null). */
    private final byte[] value;

    /**
     * Creates a new record header with the given key and value.
     *
     * @param key   the header key (must not be null)
     * @param value the header value (may be null)
     */
    public RecordHeaderAdapter(final String key, final byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public byte[] value() {
        return value;
    }
}
