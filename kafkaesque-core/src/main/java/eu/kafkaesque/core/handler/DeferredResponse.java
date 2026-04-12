package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.connection.ClientConnection;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

/**
 * A serialised Kafka response that could not be written immediately and must be
 * dispatched to the client at a later point in the event loop.
 *
 * <p>Used by the consumer-group rebalance logic: a {@code JoinGroup} response is held
 * until all members in the same group have joined (or the rebalance window closes), then
 * each pending response is enqueued as a {@link DeferredResponse} and flushed by the
 * NIO event loop on its next iteration.</p>
 */
@EqualsAndHashCode
@ToString
final class DeferredResponse {

    /** The client connection to write the response to. */
    private final ClientConnection connection;

    /** The selection key for the connection; used to register {@code OP_WRITE}. */
    private final SelectionKey key;

    /** The serialised response buffer, positioned ready to read. */
    private final ByteBuffer response;

    /**
     * Creates a new {@code DeferredResponse}.
     *
     * @param connection the client connection to write the response to
     * @param key        the selection key for the connection; used to register {@code OP_WRITE}
     * @param response   the serialised response buffer, positioned ready to read
     */
    DeferredResponse(final ClientConnection connection, final SelectionKey key, final ByteBuffer response) {
        this.connection = connection;
        this.key = key;
        this.response = response;
    }

    /**
     * Returns the client connection to write the response to.
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

    /**
     * Returns the serialised response buffer.
     *
     * @return the response buffer
     */
    ByteBuffer response() {
        return response;
    }
}
