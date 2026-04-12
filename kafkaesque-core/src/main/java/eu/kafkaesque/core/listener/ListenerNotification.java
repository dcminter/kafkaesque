package eu.kafkaesque.core.listener;

import eu.kafkaesque.core.storage.StoredRecord;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Interface representing a notification that is enqueued for asynchronous
 * delivery to registered listeners.
 *
 * <p>The NIO event-loop thread creates instances of the subtypes and places
 * them on a {@link java.util.concurrent.LinkedBlockingQueue}. A dedicated consumer
 * daemon thread drains the queue and dispatches each notification to the appropriate
 * listeners.</p>
 *
 * @see ListenerRegistry
 */
interface ListenerNotification {

    /**
     * Notification that a record was published.
     */
    @EqualsAndHashCode
    @ToString
    static final class RecordPublished implements ListenerNotification {

        /** The stored record. */
        private final StoredRecord record;

        /**
         * Creates a new {@code RecordPublished} notification.
         *
         * @param record the stored record
         */
        RecordPublished(final StoredRecord record) {
            this.record = record;
        }

        /**
         * Returns the stored record.
         *
         * @return the stored record
         */
        StoredRecord record() {
            return record;
        }
    }

    /**
     * Notification that a topic was created.
     */
    @EqualsAndHashCode
    @ToString
    static final class TopicCreated implements ListenerNotification {

        /** The name of the new topic. */
        private final String topicName;

        /**
         * Creates a new {@code TopicCreated} notification.
         *
         * @param topicName the name of the new topic
         */
        TopicCreated(final String topicName) {
            this.topicName = topicName;
        }

        /**
         * Returns the name of the new topic.
         *
         * @return the name of the new topic
         */
        String topicName() {
            return topicName;
        }
    }

    /**
     * Notification that a transaction completed.
     */
    @EqualsAndHashCode
    @ToString
    static final class TransactionCompleted implements ListenerNotification {

        /** The transactional producer ID. */
        private final String transactionalId;

        /** {@code true} if committed, {@code false} if aborted. */
        private final boolean committed;

        /**
         * Creates a new {@code TransactionCompleted} notification.
         *
         * @param transactionalId the transactional producer ID
         * @param committed       {@code true} if committed, {@code false} if aborted
         */
        TransactionCompleted(final String transactionalId, final boolean committed) {
            this.transactionalId = transactionalId;
            this.committed = committed;
        }

        /**
         * Returns the transactional producer ID.
         *
         * @return the transactional producer ID
         */
        String transactionalId() {
            return transactionalId;
        }

        /**
         * Returns whether the transaction was committed.
         *
         * @return {@code true} if committed, {@code false} if aborted
         */
        boolean committed() {
            return committed;
        }
    }

    /**
     * Poison pill that signals the consumer thread to shut down.
     */
    @EqualsAndHashCode
    @ToString
    static final class Shutdown implements ListenerNotification {

        /**
         * Creates a new {@code Shutdown} notification.
         */
        Shutdown() {
        }
    }
}
