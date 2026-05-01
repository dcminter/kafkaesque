package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.connection.ClientConnection;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.channels.SelectionKey;

/**
 * Carries everything an {@link ApiRequestHandler} may need to produce a response for a single
 * Kafka request: the parsed API key and header, the parsed request body, and the originating
 * client connection and selection key.
 *
 * <p>The request body is exposed as a typed {@link AbstractRequest} (parsed once at the
 * dispatch layer via {@link AbstractRequest#parseRequest(ApiKeys, short, java.nio.ByteBuffer)})
 * so that handlers can read fields directly via {@link AbstractRequest#data()} without
 * re-parsing the buffer, and so that the dispatcher can use
 * {@link AbstractRequest#getErrorResponse(int, Throwable)} to build a properly-shaped error
 * response if the handler fails.</p>
 *
 * <p>A single context shape lets every handler share the signature
 * {@code ByteBuffer handle(RequestContext)}, even though most handlers ignore
 * {@link #connection()} and {@link #key()}. Those fields exist to support the deferred-response
 * mechanism used by {@code JOIN_GROUP} and {@code SYNC_GROUP}, where the response is queued and
 * written later on the event loop.</p>
 */
@EqualsAndHashCode
@ToString
@Getter
@RequiredArgsConstructor
final class RequestContext {

    /** The API key identifying the request type. */
    private final ApiKeys apiKey;

    /** The parsed Kafka request header. */
    private final RequestHeader header;

    /** The parsed Kafka request body. */
    private final AbstractRequest request;

    /** The originating client connection; used by handlers that defer responses. */
    private final ClientConnection connection;

    /** The selection key for the connection; used by handlers that defer responses. */
    private final SelectionKey key;
}
