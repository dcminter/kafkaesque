package eu.kafkaesque.core.listener;

import eu.kafkaesque.core.storage.StoredRecord;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
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
    @Getter
    @RequiredArgsConstructor
    static final class RecordPublished implements ListenerNotification {

        /** The stored record. */
        private final StoredRecord record;
    }

    /**
     * Notification that a topic was created.
     */
    @EqualsAndHashCode
    @ToString
    @Getter
    @RequiredArgsConstructor
    static final class TopicCreated implements ListenerNotification {

        /** The name of the new topic. */
        private final String topicName;
    }

    /**
     * Notification that a transaction completed.
     */
    @EqualsAndHashCode
    @ToString
    @Getter
    @RequiredArgsConstructor
    static final class TransactionCompleted implements ListenerNotification {

        /** The transactional producer ID. */
        private final String transactionalId;

        /** {@code true} if committed, {@code false} if aborted. */
        private final boolean committed;
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
