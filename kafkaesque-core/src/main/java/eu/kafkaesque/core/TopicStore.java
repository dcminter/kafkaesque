package eu.kafkaesque.core;

import org.apache.kafka.common.Uuid;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry of topics known to Kafkaesque.
 *
 * <p>Topics are registered here when explicitly created via the admin API.
 * The registry is consulted when generating METADATA and
 * DESCRIBE_TOPIC_PARTITIONS responses so that partition counts and topic IDs
 * reflect what was requested at creation time, and so that
 * {@code AdminClient.listTopics()} returns all known topics.</p>
 *
 * @see AdminApiHandler
 * @see ClusterApiHandler
 */
public final class TopicStore {

    /**
     * Describes a topic as it was created.
     *
     * @param name              the topic name
     * @param numPartitions     the number of partitions
     * @param replicationFactor the replication factor
     * @param topicId           the stable UUID assigned to this topic at creation time
     */
    public record TopicDefinition(String name, int numPartitions, short replicationFactor, Uuid topicId) {}

    private final Map<String, TopicDefinition> topics = new ConcurrentHashMap<>();

    /**
     * Registers a new topic with the given configuration, assigning it a stable random UUID.
     *
     * <p>If a topic with the same name already exists its existing definition is
     * retained; this method is effectively idempotent for a given name.</p>
     *
     * @param name              the topic name
     * @param numPartitions     the number of partitions
     * @param replicationFactor the replication factor
     */
    public void createTopic(final String name, final int numPartitions, final short replicationFactor) {
        topics.putIfAbsent(name, new TopicDefinition(name, numPartitions, replicationFactor, Uuid.randomUuid()));
    }

    /**
     * Returns whether a topic with the given name is registered.
     *
     * @param name the topic name
     * @return {@code true} if the topic is known
     */
    public boolean hasTopic(final String name) {
        return topics.containsKey(name);
    }

    /**
     * Returns the definition of the named topic, if registered.
     *
     * @param name the topic name
     * @return the topic definition, or {@link Optional#empty()} if not registered
     */
    public Optional<TopicDefinition> getTopic(final String name) {
        return Optional.ofNullable(topics.get(name));
    }

    /**
     * Returns all registered topic definitions.
     *
     * @return an unmodifiable view of all topic definitions
     */
    public Collection<TopicDefinition> getTopics() {
        return Collections.unmodifiableCollection(topics.values());
    }
}
