package eu.kafkaesque.junit4;

/**
 * Defines a topic to be pre-created on the {@link eu.kafkaesque.core.KafkaesqueServer} before
 * any test code runs.
 *
 * <p>This record is the programmatic equivalent of the JUnit 5
 * {@code @KafkaesqueTopic} annotation. It is used with the {@link KafkaesqueRule.Builder}
 * to declare topics that should exist before tests execute.</p>
 *
 * <p>Example — single partition, replication factor 1:</p>
 * <pre>{@code
 * new TopicDefinition("orders")
 * }</pre>
 *
 * <p>Example — custom partition count:</p>
 * <pre>{@code
 * new TopicDefinition("events", 5)
 * }</pre>
 *
 * <p>Example — full control:</p>
 * <pre>{@code
 * new TopicDefinition("events", 5, (short) 1)
 * }</pre>
 *
 * @param name              the topic name
 * @param numPartitions     the number of partitions
 * @param replicationFactor the replication factor
 * @see KafkaesqueRule.Builder#topics(TopicDefinition...)
 */
public record TopicDefinition(String name, int numPartitions, short replicationFactor) {

    /**
     * Creates a topic definition with a single partition and replication factor of 1.
     *
     * @param name the topic name
     */
    public TopicDefinition(final String name) {
        this(name, 1, (short) 1);
    }

    /**
     * Creates a topic definition with the given partition count and replication factor of 1.
     *
     * @param name          the topic name
     * @param numPartitions the number of partitions
     */
    public TopicDefinition(final String name, final int numPartitions) {
        this(name, numPartitions, (short) 1);
    }
}
