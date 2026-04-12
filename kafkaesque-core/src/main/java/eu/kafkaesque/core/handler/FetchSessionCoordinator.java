package eu.kafkaesque.core.handler;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.FetchRequestData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Map.copyOf;
import static org.apache.kafka.common.protocol.Errors.FETCH_SESSION_ID_NOT_FOUND;
import static org.apache.kafka.common.protocol.Errors.INVALID_FETCH_SESSION_EPOCH;
import static org.apache.kafka.common.protocol.Errors.NONE;

/**
 * Manages Kafka fetch sessions as defined by KIP-227.
 *
 * <p>Fetch sessions allow consumers to send incremental fetch requests, avoiding repeated
 * specification of unchanged topic-partition fetch parameters. Each session tracks the full
 * set of partitions a consumer wants to fetch, keyed by session ID. Clients establish a
 * session with an initial full fetch ({@code sessionId=0, sessionEpoch=0}), then send
 * incremental updates containing only changed partitions.</p>
 *
 * <p>The special epoch value {@code -1} ({@link #FINAL_EPOCH}) signals that the client
 * wants to close the session. The epoch value {@code 0} ({@link #INITIAL_EPOCH}) combined
 * with {@code sessionId=0} signals that the client wants to create a new session.</p>
 *
 * <p>This class is thread-safe.</p>
 *
 * @see ConsumerDataApiHandler
 */
@Slf4j
final class FetchSessionCoordinator {

    /** The epoch value clients use when requesting a new session (combined with {@code sessionId=0}). */
    static final int INITIAL_EPOCH = 0;

    /** The epoch value clients use to close an existing session, or to indicate legacy (no-session) mode. */
    static final int FINAL_EPOCH = -1;

    /**
     * Identifies a single partition within a topic inside a fetch session.
     */
    @EqualsAndHashCode
    @ToString
    static final class TopicPartitionKey {

        /** The topic name. */
        private final String topic;

        /** The partition index. */
        private final int partition;

        /**
         * Creates a new {@code TopicPartitionKey}.
         *
         * @param topic     the topic name
         * @param partition the partition index
         */
        TopicPartitionKey(final String topic, final int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        /**
         * Returns the topic name.
         *
         * @return the topic name
         */
        String topic() {
            return topic;
        }

        /**
         * Returns the partition index.
         *
         * @return the partition index
         */
        int partition() {
            return partition;
        }
    }

    /**
     * Per-partition fetch state tracked within a session.
     */
    @EqualsAndHashCode
    @ToString
    static final class PartitionFetchState {

        /** The offset from which to start fetching records. */
        private final long fetchOffset;

        /** The maximum bytes to return for this partition. */
        private final int maxBytes;

        /**
         * Creates a new {@code PartitionFetchState}.
         *
         * @param fetchOffset the offset from which to start fetching records
         * @param maxBytes    the maximum bytes to return for this partition
         */
        PartitionFetchState(final long fetchOffset, final int maxBytes) {
            this.fetchOffset = fetchOffset;
            this.maxBytes = maxBytes;
        }

        /**
         * Returns the offset from which to start fetching records.
         *
         * @return the fetch offset
         */
        long fetchOffset() {
            return fetchOffset;
        }

        /**
         * Returns the maximum bytes to return for this partition.
         *
         * @return the maximum bytes
         */
        int maxBytes() {
            return maxBytes;
        }
    }

    /**
     * Immutable snapshot of an active fetch session.
     */
    @EqualsAndHashCode
    @ToString
    static final class FetchSession {

        /** The unique session identifier assigned by this coordinator. */
        private final int sessionId;

        /** The next expected epoch from the client (starts at 1 after session creation). */
        private final int nextEpoch;

        /** The currently tracked partitions and their fetch state. */
        private final Map<TopicPartitionKey, PartitionFetchState> partitions;

        /**
         * Creates a new {@code FetchSession}.
         *
         * @param sessionId  the unique session identifier assigned by this coordinator
         * @param nextEpoch  the next expected epoch from the client (starts at 1 after session creation)
         * @param partitions the currently tracked partitions and their fetch state
         */
        FetchSession(final int sessionId, final int nextEpoch,
                     final Map<TopicPartitionKey, PartitionFetchState> partitions) {
            this.sessionId = sessionId;
            this.nextEpoch = nextEpoch;
            this.partitions = partitions;
        }

        /**
         * Returns the unique session identifier.
         *
         * @return the session ID
         */
        int sessionId() {
            return sessionId;
        }

        /**
         * Returns the next expected epoch from the client.
         *
         * @return the next epoch
         */
        int nextEpoch() {
            return nextEpoch;
        }

        /**
         * Returns the currently tracked partitions and their fetch state.
         *
         * @return the partitions map
         */
        Map<TopicPartitionKey, PartitionFetchState> partitions() {
            return partitions;
        }
    }

    /**
     * Result of a session update operation.
     */
    @EqualsAndHashCode
    @ToString
    static final class SessionResult {

        /** The updated session, or {@code null} if an error occurred. */
        private final FetchSession session;

        /** The Kafka error code (0 for success, non-zero for error). */
        private final short errorCode;

        /**
         * Creates a new {@code SessionResult}.
         *
         * @param session   the updated session, or {@code null} if an error occurred
         * @param errorCode the Kafka error code (0 for success, non-zero for error)
         */
        SessionResult(final FetchSession session, final short errorCode) {
            this.session = session;
            this.errorCode = errorCode;
        }

        /**
         * Returns the updated session, or {@code null} if an error occurred.
         *
         * @return the session
         */
        FetchSession session() {
            return session;
        }

        /**
         * Returns the Kafka error code.
         *
         * @return the error code
         */
        short errorCode() {
            return errorCode;
        }

        /**
         * Returns {@code true} if this result represents an error condition.
         *
         * @return {@code true} if the error code is non-zero
         */
        boolean isError() {
            return errorCode != 0;
        }
    }

    private final AtomicInteger nextSessionId = new AtomicInteger(1);
    private final Map<Integer, FetchSession> sessions = new ConcurrentHashMap<>();

    /**
     * Creates a new fetch session for the given initial partition set.
     *
     * <p>A unique session ID is assigned and the session epoch is initialised to 1,
     * i.e. the client must send epoch 1 on the next incremental fetch request.</p>
     *
     * @param partitions the initial set of partitions and their fetch state
     * @return the newly created session
     */
    FetchSession createSession(final Map<TopicPartitionKey, PartitionFetchState> partitions) {
        final int sessionId = nextSessionId.getAndIncrement();
        final var session = new FetchSession(sessionId, 1, copyOf(partitions));
        sessions.put(sessionId, session);
        log.debug("Created fetch session {}, tracking {} partition(s)", sessionId, partitions.size());
        return session;
    }

    /**
     * Validates and updates an existing fetch session for an incremental fetch.
     *
     * <p>The incoming partition updates are merged into the existing session's partition map.
     * Any partitions listed in {@code forgottenTopics} are removed from the merged map.
     * The session epoch is advanced by one. On success the updated session replaces the old
     * one in the session store.</p>
     *
     * <p>Returns an error result if the session cannot be found or if the epoch does not
     * match the expected value.</p>
     *
     * <p>This operation is performed atomically via {@link java.util.concurrent.ConcurrentHashMap#compute}
     * to prevent a time-of-check/time-of-use race between epoch validation and the session write-back.</p>
     *
     * @param sessionId      the session ID from the request
     * @param epoch          the session epoch from the request
     * @param updates        partitions with updated or new fetch states
     * @param forgottenTopics the topic-partitions to remove from the session
     * @return a {@link SessionResult} containing the updated session, or an error code
     */
    SessionResult updateSession(
            final int sessionId,
            final int epoch,
            final Map<TopicPartitionKey, PartitionFetchState> updates,
            final List<FetchRequestData.ForgottenTopic> forgottenTopics) {
        final SessionResult[] holder = new SessionResult[1];
        sessions.compute(sessionId, (id, session) -> {
            if (session == null) {
                log.debug("Fetch session {} not found", id);
                holder[0] = new SessionResult(null, FETCH_SESSION_ID_NOT_FOUND.code());
                return null;
            }
            if (epoch != session.nextEpoch()) {
                log.debug("Invalid epoch for session {}: expected {}, got {}", id, session.nextEpoch(), epoch);
                holder[0] = new SessionResult(null, INVALID_FETCH_SESSION_EPOCH.code());
                return session;
            }
            final var merged = new HashMap<>(session.partitions());
            merged.putAll(updates);
            removeForgottenPartitions(merged, forgottenTopics);
            final var updated = new FetchSession(id, epoch + 1, copyOf(merged));
            log.debug("Updated fetch session {}, next epoch is {}", id, epoch + 1);
            holder[0] = new SessionResult(updated, NONE.code());
            return updated;
        });
        return holder[0];
    }

    /**
     * Closes and removes a fetch session.
     *
     * @param sessionId the ID of the session to close
     * @return {@code true} if the session was found and removed,
     *         {@code false} if no session with the given ID existed
     */
    boolean closeSession(final int sessionId) {
        final var removed = sessions.remove(sessionId);
        if (removed != null) {
            log.debug("Closed fetch session {}", sessionId);
            return true;
        }
        return false;
    }

    /**
     * Clears all active fetch sessions.
     */
    void clear() {
        sessions.clear();
    }

    /**
     * Removes the specified forgotten topic-partitions from the partition map.
     *
     * @param partitions      the mutable partition map to update
     * @param forgottenTopics the topic-partitions to remove
     */
    private void removeForgottenPartitions(
            final Map<TopicPartitionKey, PartitionFetchState> partitions,
            final List<FetchRequestData.ForgottenTopic> forgottenTopics) {
        forgottenTopics.forEach(ft -> ft.partitions().forEach(
            partition -> partitions.remove(new TopicPartitionKey(ft.topic(), partition))));
    }
}
