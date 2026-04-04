package eu.kafkaesque.core.storage;

/**
 * Represents the commit state of a transactionally-produced record.
 *
 * <p>Non-transactional records carry no state and are always visible to consumers
 * at any isolation level. Transactional records progress through these states as the
 * owning transaction is processed by the transaction coordinator.</p>
 */
public enum TransactionState {

    /**
     * The record has been written to the broker but the owning transaction has not yet
     * been committed or aborted.
     *
     * <p>Records in this state are visible only to {@code READ_UNCOMMITTED} consumers;
     * {@code READ_COMMITTED} consumers cannot see records beyond the
     * last-stable-offset (LSO), which is pinned at the lowest pending offset.</p>
     */
    PENDING,

    /**
     * The owning transaction was successfully committed.
     *
     * <p>Records in this state are visible to consumers at all isolation levels.</p>
     */
    COMMITTED,

    /**
     * The owning transaction was aborted.
     *
     * <p>Records in this state are never returned to consumers regardless of
     * isolation level.</p>
     */
    ABORTED
}
