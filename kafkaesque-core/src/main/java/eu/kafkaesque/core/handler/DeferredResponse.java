package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.connection.ClientConnection;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
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
@Getter
@RequiredArgsConstructor
final class DeferredResponse {

    /** The client connection to write the response to. */
    private final ClientConnection connection;

    /** The selection key for the connection; used to register {@code OP_WRITE}. */
    private final SelectionKey key;

    /** The serialised response buffer, positioned ready to read. */
    private final ByteBuffer response;
}
