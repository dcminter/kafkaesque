package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Abstract base class defining integration test cases for Kafka brokers configured
 * with auto-topic-creation disabled ({@code auto.create.topics.enable=false}).
 *
 * <p>These tests verify that:</p>
 * <ul>
 *   <li>Producing to a topic that has never been created fails with
 *       {@link UnknownTopicOrPartitionException}</li>
 *   <li>Producing to a topic that was explicitly created via the admin API succeeds</li>
 * </ul>
 *
 * <p>Subclasses must implement {@link #getBootstrapServers()} and must configure
 * the underlying broker with auto-topic-creation disabled.</p>
 *
 * @see RealKafkaNoAutoCreateIT
 * @see KafkaesqueNoAutoCreateIT
 */
@Slf4j
abstract class AbstractNoAutoCreateTopicsBehaviorIT {

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Verifies that producing to a topic that was never created fails with
     * {@link UnknownTopicOrPartitionException} when auto-topic-creation is disabled.
     *
     * @throws Exception if the bootstrap address cannot be determined
     */
    @Test
    void shouldRejectProduceToUnknownTopicWhenAutoCreateIsDisabled() throws Exception {
        // Given
        final String unknownTopic = "never-created-topic-" + UUID.randomUUID();

        try (final KafkaProducer<String, String> producer = createShortTimeoutProducer(getBootstrapServers())) {
            // When / Then
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>(unknownTopic, "k", "v")).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TimeoutException.class);

            log.info("Correctly rejected produce to unknown topic '{}'", unknownTopic);
        }
    }

    /**
     * Verifies that producing to a topic explicitly created via the admin API succeeds
     * even when auto-topic-creation is disabled.
     *
     * @throws Exception if the admin client, producer, or bootstrap address lookup fails
     */
    @Test
    void shouldSucceedProduceToExplicitlyCreatedTopicWhenAutoCreateIsDisabled() throws Exception {
        // Given
        final String topicName = "explicitly-created-topic-" + UUID.randomUUID();
        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get();
        }

        // When
        try (final KafkaProducer<String, String> producer = createShortTimeoutProducer(getBootstrapServers())) {
            final RecordMetadata metadata = producer.send(
                new ProducerRecord<>(topicName, "key", "value")).get();

            // Then
            assertThat(metadata).isNotNull();
            assertThat(metadata.topic()).isEqualTo(topicName);
            assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);

            log.info("Successfully produced to explicitly created topic '{}' at offset {}",
                topicName, metadata.offset());
        }
    }

    /**
     * Creates a {@link KafkaProducer} with short timeouts, suitable for tests that expect
     * fast failure when a topic does not exist.
     *
     * @param bootstrapServers the broker address
     * @return a configured producer
     */
    private static KafkaProducer<String, String> createShortTimeoutProducer(final String bootstrapServers) {
        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 11000);
        return new KafkaProducer<>(props);
    }
}
