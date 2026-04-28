package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.connection.ClientConnection;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

/**
 * Carries everything an {@link ApiRequestHandler} may need to produce a response for a single
 * Kafka request: the parsed API key and header, the request body buffer, and the originating
 * client connection and selection key.
 *
 * <p>A single context shape lets every handler share the signature
 * {@code ByteBuffer handle(RequestContext)}, even though most handlers ignore
 * {@link #connection()} and {@link #key()}. Those fields exist to support the deferred-response
 * mechanism used by {@code JOIN_GROUP} and {@code SYNC_GROUP}, where the response is queued and
 * written later on the event loop.</p>
 */
@EqualsAndHashCode
@ToString
final class RequestContext {

    /** The API key identifying the request type. */
    private final ApiKeys apiKey;

    /** The parsed Kafka request header. */
    private final RequestHeader header;

    /** The request body buffer, positioned immediately after the header. */
    private final ByteBuffer buffer;

    /** The originating client connection; used by handlers that defer responses. */
    private final ClientConnection connection;

    /** The selection key for the connection; used by handlers that defer responses. */
    private final SelectionKey key;

    /**
     * Creates a new {@code RequestContext}.
     *
     * @param apiKey     the API key identifying the request type
     * @param header     the parsed Kafka request header
     * @param buffer     the request body buffer, positioned immediately after the header
     * @param connection the originating client connection
     * @param key        the selection key for the connection
     */
    RequestContext(
            final ApiKeys apiKey,
            final RequestHeader header,
            final ByteBuffer buffer,
            final ClientConnection connection,
            final SelectionKey key) {
        this.apiKey = apiKey;
        this.header = header;
        this.buffer = buffer;
        this.connection = connection;
        this.key = key;
    }

    /**
     * Returns the API key identifying the request type.
     *
     * @return the API key
     */
    ApiKeys apiKey() {
        return apiKey;
    }

    /**
     * Returns the parsed Kafka request header.
     *
     * @return the request header
     */
    RequestHeader header() {
        return header;
    }

    /**
     * Returns the request body buffer.
     *
     * @return the buffer positioned immediately after the request header
     */
    ByteBuffer buffer() {
        return buffer;
    }

    /**
     * Returns the originating client connection.
     *
     * @return the client connection
     */
    ClientConnection connection() {
        return connection;
    }

    /**
     * Returns the selection key for the connection.
     *
     * @return the selection key
     */
    SelectionKey key() {
        return key;
    }
}
