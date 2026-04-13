package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.CompressionType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import static eu.kafkaesque.it.KafkaCompat.poll;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Abstract base class defining compression behavior tests.
 *
 * <p>Each parameterised case creates a topic via {@link AdminClient} with a specific
 * {@code compression.type} config, produces two records, and asserts that a consumer
 * receives them with correct key and value. Decompression is handled transparently
 * by the Kafka client library, so the test is backend-agnostic.</p>
 *
 * <p>Subclasses must implement {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractCompressionBehaviorIT {

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Verifies that records produced to a topic with a specific {@code compression.type}
     * config can be consumed and that key and value are preserved faithfully.
     *
     * @param compressionType the Kafka compression type name (e.g. {@code "gzip"})
     * @throws Exception if the admin client or producer send fails
     */
    @ParameterizedTest(name = "compression={0}")
    @ValueSource(strings = {"uncompressed", "gzip", "snappy", "lz4", "zstd"})
    void shouldProduceAndConsumeWithCompression(final String compressionType) throws Exception {
        assumeTrue(isCompressionSupported(compressionType),
            compressionType + " not supported by this Kafka client version");
        final String bootstrapServers = getBootstrapServers();
        final String topicName = "compression-test-" + compressionType + "-" + UUID.randomUUID();

        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            final var newTopic = new NewTopic(topicName, 1, (short) 1)
                .configs(Map.of("compression.type", compressionType));
            adminClient.createTopics(List.of(newTopic)).all().get();
        }

        try (final KafkaProducer<String, String> producer =
                    KafkaTestClientFactory.createProducer(bootstrapServers);
             final KafkaConsumer<String, String> consumer =
                    KafkaTestClientFactory.createConsumer(bootstrapServers)) {

            producer.send(new ProducerRecord<>(topicName, "k1", "v1")).get();
            producer.send(new ProducerRecord<>(topicName, "k2", "v2")).get();
            producer.flush();

            consumer.subscribe(List.of(topicName));

            final List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(10, SECONDS).until(() -> {
                poll(consumer, 100).forEach(received::add);
                return received.size() >= 2;
            });

            assertThat(received).hasSize(2);
            assertThat(received.get(0).key()).isEqualTo("k1");
            assertThat(received.get(0).value()).isEqualTo("v1");
            assertThat(received.get(1).key()).isEqualTo("k2");
            assertThat(received.get(1).value()).isEqualTo("v2");

            log.info("compression={}: consumed {} messages successfully", compressionType, received.size());
        }
    }

    /**
     * Checks whether the given compression type is supported by the current Kafka client.
     *
     * <p>Zstd was introduced in Kafka 2.1.0 (compression type id 4); older clients throw
     * {@link IllegalArgumentException} when encountering it.</p>
     *
     * @param compressionType the compression type name to check
     * @return {@code true} if the type is supported
     */
    private static boolean isCompressionSupported(final String compressionType) {
        try {
            CompressionType.forName(compressionType);
            return true;
        } catch (final IllegalArgumentException e) {
            return false;
        }
    }
}
