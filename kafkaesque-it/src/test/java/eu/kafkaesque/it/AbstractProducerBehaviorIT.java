package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract base class defining the producer integration test suite.
 *
 * <p>Covers synchronous publishing of single and multiple records. Subclasses must
 * implement {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractProducerBehaviorIT {

    /** Per-test producer; created fresh in {@link #setUpProducer()}. */
    private KafkaProducer<String, String> producer;

    /** Per-test topic name; unique per invocation to avoid cross-test interference. */
    private String topicName;

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Creates a fresh producer for the current test.
     *
     * @throws Exception if the producer cannot be created
     */
    @BeforeEach
    void setUpProducer() throws Exception {
        topicName = "test-topic-" + UUID.randomUUID();
        producer = KafkaTestClientFactory.createProducer(getBootstrapServers());
    }

    /**
     * Closes the Kafka producer after each test.
     */
    @AfterEach
    void tearDownProducer() {
        if (producer != null) {
            producer.close();
        }
    }

    /**
     * Verifies that a single event can be published synchronously and that the
     * returned metadata correctly reflects the topic, partition, and offset.
     *
     * @throws ExecutionException   if the producer future fails
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    @Test
    void shouldPublishEventsSynchronouslyToKafka() throws ExecutionException, InterruptedException {
        // Given
        final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "test-key", "test-value");

        // When
        final RecordMetadata metadata = producer.send(record).get();

        // Then
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(topicName);
        assertThat(metadata.partition()).isGreaterThanOrEqualTo(0);
        assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);

        log.info("Published to topic={}, partition={}, offset={}",
            metadata.topic(), metadata.partition(), metadata.offset());
    }

    /**
     * Verifies that multiple events can be published synchronously and that each
     * one is acknowledged with a valid offset.
     *
     * @throws ExecutionException   if any producer future fails
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    @Test
    void shouldPublishMultipleEventsSynchronously() throws ExecutionException, InterruptedException {
        // Given
        final int messageCount = 5;
        final List<RecordMetadata> metadataList = new ArrayList<>();

        // When
        for (int i = 0; i < messageCount; i++) {
            final ProducerRecord<String, String> record =
                new ProducerRecord<>(topicName, "key-" + i, "value-" + i);
            metadataList.add(producer.send(record).get());
        }

        // Then
        assertThat(metadataList).hasSize(messageCount);
        assertThat(metadataList).allSatisfy(metadata -> {
            assertThat(metadata.topic()).isEqualTo(topicName);
            assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);
        });

        log.info("Published {} messages successfully", messageCount);
    }
}
