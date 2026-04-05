package eu.kafkaesque.core.storage;

import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link StoredRecord}.
 */
class StoredRecordTest {

    @Test
    void shouldPreserveAllConstructorArguments() {
        final var topic = "my-topic";
        final var partition = 3;
        final var offset = 42L;
        final var timestamp = 1_700_000_000_000L;
        final var key = "record-key";
        final var value = "record-value";

        final var record = new StoredRecord(topic, partition, offset, timestamp, List.of(), key, value);

        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.partition()).isEqualTo(partition);
        assertThat(record.offset()).isEqualTo(offset);
        assertThat(record.timestamp()).isEqualTo(timestamp);
        assertThat(record.key()).isEqualTo(key);
        assertThat(record.value()).isEqualTo(value);
        assertThat(record.headers()).isEmpty();
    }

    @Test
    void shouldAcceptNullKeyAndValue() {
        final var record = new StoredRecord("topic", 0, 0L, 0L, List.of(), null, null);

        assertThat(record.key()).isNull();
        assertThat(record.value()).isNull();
    }

    @Test
    void shouldNormaliseNullHeadersToEmptyList() {
        final var record = new StoredRecord("topic", 0, 0L, 0L, null, "key", "value");

        assertThat(record.headers()).isNotNull().isEmpty();
    }

    @Test
    void shouldReturnUnmodifiableHeaders() {
        final var mutableHeaders = new ArrayList<Header>();
        final var record = new StoredRecord("topic", 0, 0L, 0L, mutableHeaders, "key", "value");

        assertThat(record.headers()).isUnmodifiable();
    }

    @Test
    void timestampAsInstant_shouldConvertFromEpochMillis() {
        final var epochMillis = 1_700_000_000_000L;
        final var record = new StoredRecord("topic", 0, 0L, epochMillis, List.of(), "key", "value");

        assertThat(record.timestampAsInstant()).isEqualTo(Instant.ofEpochMilli(epochMillis));
    }
}
