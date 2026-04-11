package eu.kafkaesque.core.listener;

/**
 * Listener that receives a callback when a transaction is committed or aborted.
 *
 * <p>Implementations should complete quickly to avoid blocking the server's NIO
 * event-loop thread.</p>
 *
 * @see ListenerRegistry
 */
@FunctionalInterface
public interface TransactionCompletedListener {

    /**
     * Called when a transaction has been committed or aborted.
     *
     * @param transactionalId the transactional producer ID that owned the transaction
     * @param committed       {@code true} if the transaction was committed,
     *                        {@code false} if it was aborted
     */
    void onTransactionCompleted(String transactionalId, boolean committed);
}
