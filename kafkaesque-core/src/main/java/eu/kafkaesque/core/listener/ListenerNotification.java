package eu.kafkaesque.core.listener;

import eu.kafkaesque.core.storage.StoredRecord;

/**
 * Sealed interface representing a notification that is enqueued for asynchronous
 * delivery to registered listeners.
 *
 * <p>The NIO event-loop thread creates instances of the permitted subtypes and places
 * them on a {@link java.util.concurrent.LinkedBlockingQueue}. A dedicated consumer
 * daemon thread drains the queue and dispatches each notification to the appropriate
 * listeners.</p>
 *
 * @see ListenerRegistry
 */
sealed interface ListenerNotification {

    /**
     * Notification that a record was published.
     *
     * @param record the stored record
     */
    record RecordPublished(StoredRecord record) implements ListenerNotification {}

    /**
     * Notification that a topic was created.
     *
     * @param topicName the name of the new topic
     */
    record TopicCreated(String topicName) implements ListenerNotification {}

    /**
     * Notification that a transaction completed.
     *
     * @param transactionalId the transactional producer ID
     * @param committed       {@code true} if committed, {@code false} if aborted
     */
    record TransactionCompleted(String transactionalId, boolean committed) implements ListenerNotification {}

    /**
     * Poison pill that signals the consumer thread to shut down.
     */
    record Shutdown() implements ListenerNotification {}
}
