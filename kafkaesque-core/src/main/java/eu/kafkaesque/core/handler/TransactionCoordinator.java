package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.EventStore;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages transactional producer state for the Kafkaesque mock server.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Assigning stable producer IDs and epochs via {@link #initProducerId(String)}</li>
 *   <li>Committing or aborting open transactions via {@link #endTransaction(String, boolean)}</li>
 * </ul>
 *
 * <p>Each unique {@code transactionalId} is assigned a persistent producer ID. Subsequent
 * {@code INIT_PRODUCER_ID} calls for the same ID bump the epoch, which fences any prior
 * incarnation of the producer.</p>
 *
 * <p>This class is thread-safe.</p>
 */
@Slf4j
public final class TransactionCoordinator {

    /**
     * Immutable producer identity (ID + epoch) assigned to a transactional ID.
     *
     * @param producerId the stable producer ID
     * @param epoch      the producer epoch; incremented on each {@code INIT_PRODUCER_ID}
     */
    public record ProducerIdAndEpoch(long producerId, short epoch) {}

    /**
     * A single offset-commit entry buffered by a {@code TXN_OFFSET_COMMIT} request.
     *
     * <p>The commit is held pending until the owning transaction ends: on commit the
     * offset becomes visible to consumers; on abort it is discarded.</p>
     *
     * @param groupId        the consumer group ID
     * @param topic          the topic name
     * @param partitionIndex the partition index
     * @param offset         the committed offset value
     */
    public record PendingOffsetCommit(String groupId, String topic, int partitionIndex, long offset) {}

    /** Internal state kept per transactional ID. */
    private record ProducerState(long producerId, short epoch) {}

    private final AtomicLong nextProducerId = new AtomicLong(1L);
    private final Map<String, ProducerState> producers = new ConcurrentHashMap<>();
    private final Map<String, List<PendingOffsetCommit>> pendingOffsetCommits = new ConcurrentHashMap<>();
    private final EventStore eventStore;

    /**
     * Creates a new coordinator backed by the given event store.
     *
     * @param eventStore the store used to commit or abort pending records
     */
    public TransactionCoordinator(final EventStore eventStore) {
        this.eventStore = eventStore;
    }

    /**
     * Initialises (or re-initialises) a transactional producer.
     *
     * <p>If {@code transactionalId} is {@code null} or blank an ephemeral
     * producer ID is assigned with epoch {@code 0} and nothing is persisted
     * (non-transactional producers do not have stable IDs). Otherwise the
     * existing epoch is incremented or, for new producers, epoch {@code 0}
     * is assigned.</p>
     *
     * @param transactionalId the client-supplied transactional ID; may be null or blank
     * @return the assigned producer ID and epoch
     */
    public ProducerIdAndEpoch initProducerId(final String transactionalId) {
        if (transactionalId == null || transactionalId.isBlank()) {
            final long pid = nextProducerId.getAndIncrement();
            log.debug("Assigned ephemeral producerId={}", pid);
            return new ProducerIdAndEpoch(pid, (short) 0);
        }

        final ProducerState newState = producers.compute(transactionalId, (k, existing) ->
            existing == null
                ? new ProducerState(nextProducerId.getAndIncrement(), (short) 0)
                : new ProducerState(existing.producerId(), (short) (existing.epoch() + 1)));

        log.debug("Initialised producer for transactionalId={}: producerId={}, epoch={}",
            transactionalId, newState.producerId(), newState.epoch());

        return new ProducerIdAndEpoch(newState.producerId(), newState.epoch());
    }

    /**
     * Buffers a transactional offset commit for later application or discard.
     *
     * <p>The commit is not made visible to consumers until the owning transaction
     * is committed via {@link #endTransaction(String, boolean)}. If the transaction
     * is aborted the buffered commit is discarded.</p>
     *
     * @param transactionalId the transactional producer ID
     * @param groupId         the consumer group ID
     * @param topic           the topic name
     * @param partitionIndex  the partition index
     * @param offset          the committed offset value
     */
    public void addPendingOffsetCommit(
            final String transactionalId,
            final String groupId,
            final String topic,
            final int partitionIndex,
            final long offset) {
        pendingOffsetCommits
            .computeIfAbsent(transactionalId, k -> Collections.synchronizedList(new ArrayList<>()))
            .add(new PendingOffsetCommit(groupId, topic, partitionIndex, offset));
        log.debug("Buffered TxnOffsetCommit for transactionalId={}: group={}, {}[{}]={}",
            transactionalId, groupId, topic, partitionIndex, offset);
    }

    /**
     * Removes and returns all buffered offset commits for the given transactional ID.
     *
     * <p>Callers should apply the returned commits to the group coordinator on commit,
     * or discard them on abort.</p>
     *
     * @param transactionalId the transactional producer ID
     * @return the list of pending offset commits (may be empty); never {@code null}
     */
    public List<PendingOffsetCommit> drainPendingOffsetCommits(final String transactionalId) {
        final var commits = pendingOffsetCommits.remove(transactionalId);
        return commits != null ? List.copyOf(commits) : List.of();
    }

    /**
     * Commits or aborts an open transaction, updating event-store record visibility.
     *
     * <p>On commit, all records that were stored as pending under {@code transactionalId}
     * become visible to {@code READ_COMMITTED} consumers. On abort, those records are
     * permanently hidden from all consumers.</p>
     *
     * <p>Buffered {@code TXN_OFFSET_COMMIT} entries are not applied here; callers must
     * drain them via {@link #drainPendingOffsetCommits(String)} and apply or discard
     * them according to the outcome.</p>
     *
     * @param transactionalId the transactional producer ID
     * @param commit          {@code true} to commit, {@code false} to abort
     */
    public void endTransaction(final String transactionalId, final boolean commit) {
        if (commit) {
            log.debug("Committing transaction for transactionalId={}", transactionalId);
            eventStore.commitTransaction(transactionalId);
        } else {
            log.debug("Aborting transaction for transactionalId={}", transactionalId);
            eventStore.abortTransaction(transactionalId);
        }
    }

    /**
     * Clears all producer state and buffered offset commits.
     */
    public void clear() {
        producers.clear();
        pendingOffsetCommits.clear();
    }
}
