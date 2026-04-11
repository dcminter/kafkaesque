package eu.kafkaesque.core.listener;

/**
 * Listener that receives a callback when a new topic is created on the Kafkaesque server.
 *
 * <p>The listener is invoked only for genuinely new topics; duplicate creation of an
 * already-existing topic does not trigger a callback. Implementations should complete
 * quickly to avoid blocking the server's NIO event-loop thread.</p>
 *
 * @see ListenerRegistry
 */
@FunctionalInterface
public interface TopicCreatedListener {

    /**
     * Called when a new topic has been created.
     *
     * @param topicName the name of the newly created topic
     */
    void onTopicCreated(String topicName);
}
