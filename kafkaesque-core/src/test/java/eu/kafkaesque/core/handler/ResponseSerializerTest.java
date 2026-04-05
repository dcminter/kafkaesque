package eu.kafkaesque.core.handler;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ResponseSerializer}.
 */
class ResponseSerializerTest {

    @Test
    void serialize_shouldReturnNonEmptyBuffer() {
        final var header = new RequestHeader(ApiKeys.API_VERSIONS,
            ApiKeys.API_VERSIONS.latestVersion(), "test-client", 42);
        final var data = new ApiVersionsResponseData();

        final var buffer = ResponseSerializer.serialize(header, data, ApiKeys.API_VERSIONS);

        assertThat(buffer).isNotNull();
        assertThat(buffer.remaining()).isPositive();
    }

    @Test
    void serialize_shouldEmbedCorrelationId() {
        final var correlationId = 99;
        final var header = new RequestHeader(ApiKeys.API_VERSIONS,
            ApiKeys.API_VERSIONS.latestVersion(), "test-client", correlationId);
        final var data = new ApiVersionsResponseData();

        final var buffer = ResponseSerializer.serialize(header, data, ApiKeys.API_VERSIONS);

        // Parse the response header back out and verify the correlation ID
        final var headerVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(ApiKeys.API_VERSIONS.latestVersion());
        final var parsedHeader = new ResponseHeaderData(new ByteBufferAccessor(buffer), headerVersion);
        assertThat(parsedHeader.correlationId()).isEqualTo(correlationId);
    }

    @Test
    void serialize_shouldProduceReadableBuffer_forDifferentApiKeys() {
        final var header = new RequestHeader(ApiKeys.METADATA,
            ApiKeys.METADATA.latestVersion(), "test-client", 1);
        final var data = new org.apache.kafka.common.message.MetadataResponseData();

        final var buffer = ResponseSerializer.serialize(header, data, ApiKeys.METADATA);

        assertThat(buffer).isNotNull();
        assertThat(buffer.remaining()).isPositive();
    }

    @Test
    void serialize_shouldFlipBuffer_soItIsReadableFromStart() {
        final var header = new RequestHeader(ApiKeys.API_VERSIONS,
            ApiKeys.API_VERSIONS.latestVersion(), "test-client", 1);
        final var data = new ApiVersionsResponseData();
        final var apiKey = ApiKeys.API_VERSIONS;

        final var buffer = ResponseSerializer.serialize(header, data, apiKey);

        // Verify position starts at 0 (i.e. buffer was flipped before returning)
        assertThat(buffer.position()).isZero();

        // Verify limit matches expected header+body size
        final var cache = new ObjectSerializationCache();
        final var headerData = new ResponseHeaderData().setCorrelationId(header.correlationId());
        final var headerVersion = apiKey.responseHeaderVersion(header.apiVersion());
        final var expectedSize = headerData.size(cache, headerVersion) + data.size(cache, header.apiVersion());
        assertThat(buffer.limit()).isEqualTo(expectedSize);
    }
}
