package eu.kafkaesque.core.handler;

import java.nio.ByteBuffer;

/**
 * A function that produces a serialised Kafka response for a single request.
 *
 * <p>Implementations are registered against an
 * {@link org.apache.kafka.common.protocol.ApiKeys} value in {@link KafkaProtocolHandler} and
 * invoked by the dispatcher. They share a uniform signature via {@link RequestContext} so that
 * the dispatcher can route every supported API through a single map lookup.</p>
 *
 * <p>A {@code null} return value means "do not write a response now": either the response has
 * been queued for later delivery via the deferred-response mechanism (used by {@code JOIN_GROUP}
 * and {@code SYNC_GROUP} during a group rebalance), or the request could not be handled at
 * all.</p>
 */
@FunctionalInterface
interface ApiRequestHandler {

    /**
     * Produces a serialised response for the given request.
     *
     * @param context the request context carrying the API key, header, body buffer, connection
     *                and selection key
     * @return the serialised response buffer, or {@code null} to indicate that no response should
     *         be written immediately (deferred or unhandled)
     */
    ByteBuffer handle(RequestContext context);
}
