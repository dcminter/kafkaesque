package eu.kafkaesque.core.handler;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;

/**
 * Utility class for serialising Kafka wire-protocol responses.
 *
 * <p>All methods are static; this class is not instantiable.</p>
 */
final class ResponseSerializer {

    private ResponseSerializer() {
    }

    /**
     * Serialises a flexible-version response with a proper response header.
     *
     * <p>Sizes are pre-calculated to populate the {@link ObjectSerializationCache} before
     * writing, which is required for flexible (compact) encoding.</p>
     *
     * @param requestHeader the request header (provides correlationId and apiVersion)
     * @param data          the response message to serialise
     * @param apiKey        the API key (used to determine the response header version)
     * @return the serialised response buffer
     */
    static ByteBuffer serialize(
            final RequestHeader requestHeader,
            final Message data,
            final ApiKeys apiKey) {

        final var cache = new ObjectSerializationCache();

        final var responseHeaderData = new ResponseHeaderData()
            .setCorrelationId(requestHeader.correlationId());
        final var headerVersion = apiKey.responseHeaderVersion(requestHeader.apiVersion());

        final var headerSize = responseHeaderData.size(cache, headerVersion);
        final var bodySize = data.size(cache, requestHeader.apiVersion());

        final var buffer = ByteBuffer.allocate(headerSize + bodySize);
        final var accessor = new ByteBufferAccessor(buffer);

        responseHeaderData.write(accessor, cache, headerVersion);
        data.write(accessor, cache, requestHeader.apiVersion());

        buffer.flip();
        return buffer;
    }
}
