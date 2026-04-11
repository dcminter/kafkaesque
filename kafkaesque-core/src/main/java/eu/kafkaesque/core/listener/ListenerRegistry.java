package eu.kafkaesque.core.listener;

import eu.kafkaesque.core.storage.StoredRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Queue;

/**
 * Thread-safe registry for Kafkaesque event listeners with asynchronous dispatch.
 *
 * <p>The {@code fire*} methods do not invoke listeners directly. Instead they enqueue
 * lightweight {@link ListenerNotification} objects onto a {@link LinkedBlockingQueue}.
 * A dedicated daemon thread consumes from the queue and dispatches each notification to
 * the appropriate registered listeners. This design ensures that the NIO event-loop
 * thread is never blocked by listener execution.</p>
 *
 * <p>Listener lists are backed by {@link CopyOnWriteArrayList} (declared as {@link List}
 * per project conventions) so that registration and notification can occur concurrently
 * without external synchronisation.</p>
 *
 * <p>Call {@link #close()} during server shutdown to drain remaining notifications and
 * stop the consumer thread.</p>
 *
 * @see RecordPublishedListener
 * @see TopicCreatedListener
 * @see TransactionCompletedListener
 * @see ListenerNotification
 */
@Slf4j
public final class ListenerRegistry {

    /** Maximum time in seconds to wait for the consumer thread to finish during shutdown. */
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final List<RecordPublishedListener> recordPublishedListeners = new CopyOnWriteArrayList<>();
    private final List<TopicCreatedListener> topicCreatedListeners = new CopyOnWriteArrayList<>();
    private final List<TransactionCompletedListener> transactionCompletedListeners = new CopyOnWriteArrayList<>();

    private final Queue<ListenerNotification> notificationQueue = new LinkedBlockingQueue<>();
    private final Thread consumerThread;

    /**
     * Creates a new listener registry and starts its consumer daemon thread.
     */
    public ListenerRegistry() {
        consumerThread = Thread.ofPlatform()
            .daemon(true)
            .name("kafkaesque-listener")
            .start(this::consumeNotifications);
    }

    /**
     * Registers a listener to be called when a record is published.
     *
     * @param listener the listener to register
     */
    public void addRecordPublishedListener(final RecordPublishedListener listener) {
        recordPublishedListeners.add(listener);
    }

    /**
     * Registers a listener to be called when a topic is created.
     *
     * @param listener the listener to register
     */
    public void addTopicCreatedListener(final TopicCreatedListener listener) {
        topicCreatedListeners.add(listener);
    }

    /**
     * Registers a listener to be called when a transaction completes.
     *
     * @param listener the listener to register
     */
    public void addTransactionCompletedListener(final TransactionCompletedListener listener) {
        transactionCompletedListeners.add(listener);
    }

    /**
     * Enqueues a {@link ListenerNotification.RecordPublished} notification for asynchronous
     * delivery to all registered {@link RecordPublishedListener}s.
     *
     * @param record the published record
     */
    public void fireRecordPublished(final StoredRecord record) {
        notificationQueue.add(new ListenerNotification.RecordPublished(record));
    }

    /**
     * Enqueues a {@link ListenerNotification.TopicCreated} notification for asynchronous
     * delivery to all registered {@link TopicCreatedListener}s.
     *
     * @param topicName the name of the created topic
     */
    public void fireTopicCreated(final String topicName) {
        notificationQueue.add(new ListenerNotification.TopicCreated(topicName));
    }

    /**
     * Enqueues a {@link ListenerNotification.TransactionCompleted} notification for
     * asynchronous delivery to all registered {@link TransactionCompletedListener}s.
     *
     * @param transactionalId the transactional producer ID
     * @param committed       {@code true} if committed, {@code false} if aborted
     */
    public void fireTransactionCompleted(final String transactionalId, final boolean committed) {
        notificationQueue.add(new ListenerNotification.TransactionCompleted(transactionalId, committed));
    }

    /**
     * Shuts down the consumer thread after draining any remaining notifications.
     *
     * <p>A poison-pill {@link ListenerNotification.Shutdown} is enqueued so the consumer
     * thread finishes processing all prior notifications before terminating. This method
     * blocks for up to {@value #SHUTDOWN_TIMEOUT_SECONDS} seconds waiting for the thread
     * to complete.</p>
     */
    public void close() {
        notificationQueue.add(new ListenerNotification.Shutdown());
        try {
            consumerThread.join(TimeUnit.SECONDS.toMillis(SHUTDOWN_TIMEOUT_SECONDS));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for listener consumer thread to stop");
        }
    }

    /**
     * Consumer loop that drains the notification queue and dispatches to listeners.
     *
     * <p>Runs on the dedicated {@code kafkaesque-listener} daemon thread. Blocks on
     * {@link LinkedBlockingQueue#take()} until a notification is available, then
     * dispatches it via {@link #dispatch(ListenerNotification)}. Terminates when a
     * {@link ListenerNotification.Shutdown} notification is received.</p>
     */
    private void consumeNotifications() {
        try {
            while (true) {
                final var notification = ((LinkedBlockingQueue<ListenerNotification>) notificationQueue).take();
                if (notification instanceof ListenerNotification.Shutdown) {
                    return;
                }
                dispatch(notification);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Listener consumer thread interrupted");
        }
    }

    /**
     * Dispatches a single notification to the appropriate set of registered listeners.
     *
     * <p>Exceptions thrown by individual listeners are caught and logged so that one
     * misbehaving listener cannot prevent others from being notified.</p>
     *
     * @param notification the notification to dispatch
     */
    private void dispatch(final ListenerNotification notification) {
        switch (notification) {
            case ListenerNotification.RecordPublished rp -> notifyRecordListeners(rp.record());
            case ListenerNotification.TopicCreated tc -> notifyTopicListeners(tc.topicName());
            case ListenerNotification.TransactionCompleted txn ->
                notifyTransactionListeners(txn.transactionalId(), txn.committed());
            case ListenerNotification.Shutdown ignored -> { /* handled in consumeNotifications */ }
        }
    }

    /**
     * Notifies all registered {@link RecordPublishedListener}s.
     *
     * @param record the published record
     */
    private void notifyRecordListeners(final StoredRecord record) {
        for (final var listener : recordPublishedListeners) {
            try {
                listener.onRecordPublished(record);
            } catch (final Exception e) {
                log.error("RecordPublishedListener threw an exception", e);
            }
        }
    }

    /**
     * Notifies all registered {@link TopicCreatedListener}s.
     *
     * @param topicName the name of the created topic
     */
    private void notifyTopicListeners(final String topicName) {
        for (final var listener : topicCreatedListeners) {
            try {
                listener.onTopicCreated(topicName);
            } catch (final Exception e) {
                log.error("TopicCreatedListener threw an exception", e);
            }
        }
    }

    /**
     * Notifies all registered {@link TransactionCompletedListener}s.
     *
     * @param transactionalId the transactional producer ID
     * @param committed       {@code true} if committed, {@code false} if aborted
     */
    private void notifyTransactionListeners(final String transactionalId, final boolean committed) {
        for (final var listener : transactionCompletedListeners) {
            try {
                listener.onTransactionCompleted(transactionalId, committed);
            } catch (final Exception e) {
                log.error("TransactionCompletedListener threw an exception", e);
            }
        }
    }
}
