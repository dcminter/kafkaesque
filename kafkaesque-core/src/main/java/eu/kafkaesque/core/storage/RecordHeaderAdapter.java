package eu.kafkaesque.core.storage;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Default implementation of {@link RecordHeader} backed by a key string and value byte array.
 *
 * <p>Used internally to convert wire-protocol headers into Kafkaesque's own header type.
 * Can also be used by test code that needs to construct headers programmatically.</p>
 */
@EqualsAndHashCode
@ToString
@Getter
@RequiredArgsConstructor
public final class RecordHeaderAdapter implements RecordHeader {

    /** The header key. */
    private final String key;

    /** The header value (may be null). */
    private final byte[] value;
}
