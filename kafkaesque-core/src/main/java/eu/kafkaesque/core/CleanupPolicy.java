package eu.kafkaesque.core;

/**
 * The log cleanup policy for a Kafka topic.
 *
 * <p>Determines how old log segments are handled as the partition log grows.
 * Mirrors the values accepted by the Kafka {@code cleanup.policy} topic configuration.</p>
 *
 * @see TopicStore.TopicDefinition
 */
public enum CleanupPolicy {

    /**
     * Records are deleted after the configured retention period.
     * This is the default Kafka behavior.
     */
    DELETE,

    /**
     * Log compaction: only the most-recent record for each key is retained.
     * Records with {@code null} values act as tombstones and are removed during compaction.
     */
    COMPACT,

    /**
     * Combined compaction and deletion: log compaction is applied first (retaining only the
     * latest record per key), then time-based and byte-based retention policies are applied
     * to remove segments outside the configured retention window.
     *
     * <p>Corresponds to Kafka's {@code cleanup.policy=compact,delete} (or {@code delete,compact})
     * topic configuration.</p>
     */
    COMPACT_DELETE
}
