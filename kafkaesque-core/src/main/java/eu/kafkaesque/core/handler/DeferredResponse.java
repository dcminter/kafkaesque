package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.connection.ClientConnection;

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
 *
 * @param connection the client connection to write the response to
 * @param key        the selection key for the connection; used to register {@code OP_WRITE}
 * @param response   the serialised response buffer, positioned ready to read
 */
record DeferredResponse(ClientConnection connection, SelectionKey key, ByteBuffer response) {}
