package eu.kafkaesque.core.handler;

import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link KafkaProtocolHandler#dispatchRequest} produces a properly-shaped
 * Kafka error response — rather than dropping the response on the floor — when:
 * <ol>
 *   <li>no handler is registered for the request's API key (e.g. telemetry APIs)</li>
 *   <li>a registered handler throws an unchecked exception</li>
 * </ol>
 *
 * <p>Both paths previously returned {@code null}, leaving the client in a request timeout.
 * See {@code BUGS.md} entries 1 and 2.</p>
 */
class KafkaProtocolHandlerErrorPathTest {

    private KafkaProtocolHandler handler;

    @BeforeEach
    void setUp() {
        handler = new KafkaProtocolHandler();
    }

    @AfterEach
    void tearDown() {
        handler.close();
    }

    @Test
    void unhandledApiKey_shouldReturnUnsupportedVersionResponse() {
        // GET_TELEMETRY_SUBSCRIPTIONS is intentionally not registered in the dispatcher map.
        final ApiKeys apiKey = ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS;
        final short apiVersion = apiKey.latestVersion();
        final var requestData = new GetTelemetrySubscriptionsRequestData();
        final var header = new RequestHeader(apiKey, apiVersion, "test-client", 42);
        final var buffer = serialize(requestData, apiVersion);

        final var response = handler.dispatchRequest(apiKey, header, buffer, null, null);

        assertThat(response)
            .as("dispatcher must produce a real response for an unhandled API key, "
                + "not silently drop it (BUGS.md item 2)")
            .isNotNull();

        final var parsed = parseTelemetryResponse(response, apiVersion);
        assertThat(parsed.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
    }

    @Test
    void handlerThrowing_shouldReturnErrorResponseInsteadOfDroppingIt() {
        // The METADATA handler will fail when topicStore is null only if we deliberately
        // sabotage it — the simplest way to reach the catch-block is to dispatch with a
        // header whose apiVersion is incompatible with the parsed-body version. We mimic
        // the parsing-failure scenario by handing the dispatcher a buffer whose contents
        // do not match the apiVersion in the header.
        //
        // Concretely: we serialise a v0-shaped MetadataRequestData but tell the dispatcher
        // the body is v12. AbstractRequest.parseRequest will (depending on the version)
        // either succeed with garbled fields or throw. Either way the test still verifies
        // dispatch's error contract: the response is non-null and round-trips cleanly.
        final ApiKeys apiKey = ApiKeys.METADATA;
        final short apiVersion = apiKey.latestVersion();
        // Empty body — METADATA at the latest flexible version requires fields, so parsing
        // either throws (header-only INVALID_REQUEST path) or yields something that
        // serialises a valid but mostly-empty response. Either is acceptable for this
        // assertion: we just need a *real* response, not null.
        final var emptyBody = ByteBuffer.allocate(0);
        final var header = new RequestHeader(apiKey, apiVersion, "test-client", 99);

        final var response = handler.dispatchRequest(apiKey, header, emptyBody, null, null);

        assertThat(response)
            .as("dispatcher must produce a real response when request parsing fails, "
                + "rather than silently dropping it (BUGS.md item 1)")
            .isNotNull();

        // The response must at minimum carry the correlation id from the header.
        final var correlationId = response.duplicate().getInt();
        assertThat(correlationId).isEqualTo(99);
    }

    @Test
    void wellFormedRequestForUnhandledKey_shouldRoundTripErrorResponseCleanly() {
        // Belt-and-braces variant of the first test: confirm the error response is fully
        // valid Kafka wire-format bytes by parsing it back through the matching response
        // data class.
        final ApiKeys apiKey = ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS;
        final short apiVersion = apiKey.latestVersion();
        final var requestData = new GetTelemetrySubscriptionsRequestData();
        final var header = new RequestHeader(apiKey, apiVersion, "test-client", 7);
        final var buffer = serialize(requestData, apiVersion);

        final var response = handler.dispatchRequest(apiKey, header, buffer, null, null);

        assertThat(response).isNotNull();
        skipResponseHeader(response, apiKey, apiVersion);
        final var data = new GetTelemetrySubscriptionsResponseData();
        data.read(new ByteBufferAccessor(response), apiVersion);
        assertThat(data.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
    }

    @Test
    void registeredApiKeysShouldNotIncludeTelemetry() {
        // Sanity: the unhandled-key path is what serves telemetry, so telemetry APIs must
        // not appear in the registered set.
        assertThat(handler.registeredApiKeys())
            .doesNotContain(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, ApiKeys.PUSH_TELEMETRY);
    }

    // --- helpers ---

    private static ByteBuffer serialize(final Message data, final short apiVersion) {
        final var cache = new ObjectSerializationCache();
        final var buffer = ByteBuffer.allocate(data.size(cache, apiVersion));
        data.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();
        return buffer;
    }

    private static GetTelemetrySubscriptionsResponseData parseTelemetryResponse(
            final ByteBuffer response, final short apiVersion) {
        final var copy = response.duplicate();
        skipResponseHeader(copy, ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, apiVersion);
        final var data = new GetTelemetrySubscriptionsResponseData();
        data.read(new ByteBufferAccessor(copy), apiVersion);
        return data;
    }

    private static void skipResponseHeader(
            final ByteBuffer buffer, final ApiKeys apiKey, final short apiVersion) {
        final short headerVersion = apiKey.responseHeaderVersion(apiVersion);
        final int headerBytes = (headerVersion >= 1) ? 5 : 4;
        buffer.position(buffer.position() + headerBytes);
    }
}
