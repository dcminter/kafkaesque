package eu.kafkaesque.it;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * Factory for creating {@link KafkaProducer} and {@link KafkaConsumer} instances
 * with standard test configuration.
 */
final class KafkaTestClientFactory {

    private KafkaTestClientFactory() {
    }

    /**
     * Creates a {@link KafkaProducer} configured for synchronous, at-least-once delivery.
     *
     * @param bootstrapServers the broker address
     * @return a new producer instance
     */
    static KafkaProducer<String, String> createProducer(final String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        return new KafkaProducer<>(props);
    }

    /**
     * Creates a transactional {@link KafkaProducer} with the given transactional ID.
     *
     * <p>The producer is configured for exactly-once delivery. Callers must invoke
     * {@code initTransactions()} before the first {@code beginTransaction()}.</p>
     *
     * @param bootstrapServers the broker address
     * @param transactionalId  the stable transactional ID for this producer
     * @return a new transactional producer instance
     */
    static KafkaProducer<String, String> createTransactionalProducer(
            final String bootstrapServers, final String transactionalId) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        return new KafkaProducer<>(props);
    }

    /**
     * Creates a {@link KafkaConsumer} configured to read from the earliest available offset,
     * using a randomly generated group ID so the consumer is isolated from other test consumers.
     *
     * @param bootstrapServers the broker address
     * @return a new consumer instance
     */
    static KafkaConsumer<String, String> createConsumer(final String bootstrapServers) {
        return createConsumer(bootstrapServers, "test-group-" + UUID.randomUUID());
    }

    /**
     * Creates a {@link KafkaConsumer} configured to read from the earliest available offset,
     * using the specified group ID.
     *
     * @param bootstrapServers the broker address
     * @param groupId          the consumer group ID to use
     * @return a new consumer instance
     */
    static KafkaConsumer<String, String> createConsumer(final String bootstrapServers, final String groupId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(props);
    }

    /**
     * Creates a {@link KafkaConsumer} using {@code READ_COMMITTED} isolation level.
     *
     * <p>This consumer only receives records from committed transactions and
     * non-transactional records. Records from open or aborted transactions are hidden.</p>
     *
     * @param bootstrapServers the broker address
     * @return a new READ_COMMITTED consumer instance
     */
    static KafkaConsumer<String, String> createReadCommittedConsumer(final String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-read-committed-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase());
        return new KafkaConsumer<>(props);
    }

    /**
     * Creates a {@link KafkaConsumer} using {@code READ_UNCOMMITTED} isolation level.
     *
     * <p>This consumer receives all records regardless of transaction state: records from
     * open (pending), committed, <em>and aborted</em> transactions are all returned.
     * This matches Kafka's documented contract: "consumer.poll() will return all messages,
     * even transactional messages which have been aborted."</p>
     *
     * @param bootstrapServers the broker address
     * @return a new READ_UNCOMMITTED consumer instance
     */
    static KafkaConsumer<String, String> createReadUncommittedConsumer(final String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-read-uncommitted-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase());
        return new KafkaConsumer<>(props);
    }
}
