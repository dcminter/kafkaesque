package eu.kafkaesque.core;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static eu.kafkaesque.core.CleanupPolicy.DELETE;

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
     * Configuration parameters for creating a topic.
     *
     * <p>Carries all mutable settings that can be specified when a topic is created.
     * The stable topic ID is assigned by {@link TopicStore} at registration time.</p>
     *
     * @param numPartitions     the number of partitions
     * @param replicationFactor the replication factor
     * @param compression       the compression to apply when serving FETCH responses
     * @param cleanupPolicy     the log cleanup policy
     * @param retentionMs       maximum record age in milliseconds;
     *                          {@code Long.MAX_VALUE} means unlimited
     * @param retentionBytes    maximum bytes per partition; {@code -1} means unlimited
     */
    public record TopicCreationConfig(
            int numPartitions,
            short replicationFactor,
            Compression compression,
            CleanupPolicy cleanupPolicy,
            long retentionMs,
            long retentionBytes) {

        /**
         * Returns a default config with the given partitions and replication factor,
         * no compression, delete cleanup policy, and unlimited retention.
         *
         * @param numPartitions     the number of partitions
         * @param replicationFactor the replication factor
         * @return a default {@code TopicCreationConfig}
         */
        static TopicCreationConfig defaults(final int numPartitions, final short replicationFactor) {
            return new TopicCreationConfig(
                numPartitions, replicationFactor, Compression.NONE,
                DELETE, Long.MAX_VALUE, -1L);
        }
    }

    /**
     * Describes a topic as it was created.
     *
     * @param name              the topic name
     * @param numPartitions     the number of partitions
     * @param replicationFactor the replication factor
     * @param topicId           the stable UUID assigned to this topic at creation time
     * @param compression       the compression to apply when serving FetchResponses for this topic
     * @param cleanupPolicy     the log cleanup policy
     * @param retentionMs       maximum record age in milliseconds;
     *                          {@code Long.MAX_VALUE} means unlimited
     * @param retentionBytes    maximum bytes per partition; {@code -1} means unlimited
     */
    public record TopicDefinition(
            String name,
            int numPartitions,
            short replicationFactor,
            Uuid topicId,
            Compression compression,
            CleanupPolicy cleanupPolicy,
            long retentionMs,
            long retentionBytes) {}

    private final Map<String, TopicDefinition> topics = new ConcurrentHashMap<>();

    /**
     * Registers a new topic with the given configuration, assigning it a stable random UUID.
     *
     * <p>If a topic with the same name already exists its existing definition is
     * retained; this method is effectively idempotent for a given name.</p>
     *
     * @param name   the topic name
     * @param config the creation configuration
     */
    public void createTopic(final String name, final TopicCreationConfig config) {
        topics.putIfAbsent(name, new TopicDefinition(
            name, config.numPartitions(), config.replicationFactor(), Uuid.randomUuid(),
            config.compression(), config.cleanupPolicy(), config.retentionMs(), config.retentionBytes()));
    }

    /**
     * Registers a new topic with the given configuration and compression, assigning it a stable random UUID.
     *
     * <p>FetchResponses for this topic will use the specified compression codec.
     * The cleanup policy defaults to {@link CleanupPolicy#DELETE} with unlimited retention.</p>
     *
     * <p>If a topic with the same name already exists its existing definition is
     * retained; this method is effectively idempotent for a given name.</p>
     *
     * @param name              the topic name
     * @param numPartitions     the number of partitions
     * @param replicationFactor the replication factor
     * @param compression       the compression to apply when serving FetchResponses for this topic
     */
    public void createTopic(
            final String name, final int numPartitions,
            final short replicationFactor, final Compression compression) {
        createTopic(name, new TopicCreationConfig(
            numPartitions, replicationFactor, compression,
            DELETE, Long.MAX_VALUE, -1L));
    }

    /**
     * Registers a new topic with the given configuration, assigning it a stable random UUID.
     *
     * <p>FetchResponses for this topic will use {@link Compression#NONE}.
     * The cleanup policy defaults to {@link CleanupPolicy#DELETE} with unlimited retention.</p>
     *
     * <p>If a topic with the same name already exists its existing definition is
     * retained; this method is effectively idempotent for a given name.</p>
     *
     * @param name              the topic name
     * @param numPartitions     the number of partitions
     * @param replicationFactor the replication factor
     */
    public void createTopic(final String name, final int numPartitions, final short replicationFactor) {
        createTopic(name, numPartitions, replicationFactor, Compression.NONE);
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
