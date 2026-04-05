package eu.kafkaesque.junit5;

import eu.kafkaesque.core.KafkaesqueServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that topics declared via {@link Kafkaesque#topics()} are pre-created on the
 * {@link KafkaesqueServer} before any test method runs.
 */
class KafkaesqueExtensionTopicCreationTest {

    /**
     * Verifies that a topic declared in the class-level {@link Kafkaesque} annotation exists in
     * the server's known topics before the test body runs.
     *
     * @param server the injected Kafkaesque server
     */
    @Test
    @Kafkaesque(topics = @KafkaesqueTopic(name = "pre-created-topic"))
    void shouldPreCreateDeclaredTopicOnMethodAnnotation(final KafkaesqueServer server) {
        assertThat(server.getRegisteredTopics()).contains("pre-created-topic");
    }

    /**
     * Verifies that producing to a pre-declared topic succeeds even when auto-topic-creation is
     * disabled, because the topic already exists when the test starts.
     *
     * @param server the injected Kafkaesque server
     * @throws Exception if the producer fails unexpectedly
     */
    @Test
    @Kafkaesque(autoCreateTopics = false, topics = @KafkaesqueTopic(name = "explicit-topic"))
    void shouldSucceedProducingToPreCreatedTopicWithAutoCreateDisabled(final KafkaesqueServer server) throws Exception {
        try (var producer = createShortTimeoutProducer(server.getBootstrapServers())) {
            final RecordMetadata metadata = producer.send(
                new ProducerRecord<>("explicit-topic", "key", "value")).get();

            assertThat(metadata.topic()).isEqualTo("explicit-topic");
            assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);
        }
    }

    /**
     * Verifies that producing to an undeclared topic still fails with auto-topic-creation disabled,
     * confirming that pre-created topics do not accidentally disable the restriction for other topics.
     *
     * @param server the injected Kafkaesque server
     */
    @Test
    @Kafkaesque(autoCreateTopics = false, topics = @KafkaesqueTopic(name = "known-topic"))
    void shouldRejectProduceToUndeclaredTopicEvenWhenOtherTopicsArePreCreated(final KafkaesqueServer server) throws Exception {
        try (var producer = createShortTimeoutProducer(server.getBootstrapServers())) {
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>("unknown-topic", "k", "v")).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TimeoutException.class);
        }
    }

    /**
     * Verifies that topics declared with custom partition counts are registered with the correct
     * number of partitions.
     *
     * @param server the injected Kafkaesque server
     */
    @Test
    @Kafkaesque(topics = @KafkaesqueTopic(name = "multi-partition-topic", numPartitions = 3))
    void shouldRespectCustomPartitionCount(final KafkaesqueServer server) {
        assertThat(server.getRegisteredTopics()).contains("multi-partition-topic");
    }

    /**
     * Verifies that multiple topics declared in a single annotation are all pre-created.
     *
     * @param server the injected Kafkaesque server
     */
    @Test
    @Kafkaesque(topics = {
        @KafkaesqueTopic(name = "topic-alpha"),
        @KafkaesqueTopic(name = "topic-beta")
    })
    void shouldPreCreateMultipleDeclaredTopics(final KafkaesqueServer server) {
        assertThat(server.getRegisteredTopics()).contains("topic-alpha", "topic-beta");
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
