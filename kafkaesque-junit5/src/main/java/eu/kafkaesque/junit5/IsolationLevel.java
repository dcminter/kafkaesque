package eu.kafkaesque.junit5;

/**
 * Transaction isolation level for Kafka consumers created by the Kafkaesque test framework.
 *
 * <p>This enum mirrors the semantics of {@code org.apache.kafka.common.IsolationLevel} but is
 * defined within the Kafkaesque API to avoid a compile-time dependency on a specific
 * {@code kafka-clients} version. The Kafka-internal enum moved packages across major releases,
 * so referencing it directly in an annotation would prevent the annotation from compiling with
 * older or newer client libraries.</p>
 *
 * <p>The two constants map directly to the Kafka consumer configuration values
 * {@code "read_uncommitted"} and {@code "read_committed"} used by
 * {@link org.apache.kafka.clients.consumer.ConsumerConfig#ISOLATION_LEVEL_CONFIG}.</p>
 *
 * @see KafkaesqueConsumer#isolationLevel()
 */
public enum IsolationLevel {

    /**
     * Consumers see all records, including those from aborted transactions.
     *
     * <p>This is the default isolation level and matches
     * {@code isolation.level=read_uncommitted}.</p>
     */
    READ_UNCOMMITTED,

    /**
     * Consumers see only records from committed transactions and non-transactional records.
     *
     * <p>Matches {@code isolation.level=read_committed}.</p>
     */
    READ_COMMITTED
}
