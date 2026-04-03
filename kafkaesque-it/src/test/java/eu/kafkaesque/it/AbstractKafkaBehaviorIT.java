package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract base class defining the shared Kafka behavior integration test suite.
 *
 * <p>Each test case in this class is run against every concrete subclass, ensuring
 * that both the real Kafka broker (via Testcontainers) and the Kafkaesque mock
 * implementation exhibit the same behavior. Adding a new test here automatically
 * exercises it against both backends.</p>
 *
 * <p>Subclasses must implement {@link #getBootstrapServers()} to supply the broker
 * address. Any backend lifecycle (start/stop) must be managed in
 * {@code @BeforeAll}/{@code @AfterAll} methods so the backend is ready before this
 * class's {@code @BeforeEach} calls {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractKafkaBehaviorIT {

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

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
     * Creates a fresh producer and consumer for the current test, connected to the
     * backend supplied by {@link #getBootstrapServers()}.
     *
     * @throws Exception if the clients cannot be created
     */
    @BeforeEach
    void setUpClients() throws Exception {
        final String bootstrapServers = getBootstrapServers();
        topicName = "test-topic-" + UUID.randomUUID();
        producer = createProducer(bootstrapServers);
        consumer = createConsumer(bootstrapServers);
    }

    /**
     * Closes the Kafka producer and consumer after each test.
     */
    @AfterEach
    void tearDownClients() {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    /**
     * Verifies that an {@link AdminClient} can connect to the broker and retrieve
     * cluster metadata.
     *
     * @throws Exception if the admin client cannot connect
     */
    @Test
    void shouldEstablishConnectionToKafkaBroker() throws Exception {
        // Given
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        // When / Then
        try (final AdminClient adminClient = AdminClient.create(props)) {
            final DescribeClusterResult clusterResult = adminClient.describeCluster();
            final String clusterId = clusterResult.clusterId().get();
            final int nodeCount = clusterResult.nodes().get().size();

            assertThat(clusterId).isNotNull();
            assertThat(nodeCount).isGreaterThan(0);

            log.info("Connected to cluster: id={}, nodes={}", clusterId, nodeCount);
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

    /**
     * Verifies that events published to a topic can be consumed by a subscriber,
     * and that the key, value, and topic are preserved faithfully.
     */
    @Test
    void shouldReceivePublishedEventsFromKafka() {
        // Given
        producer.send(new ProducerRecord<>(topicName, "key-1", "value-1"));
        producer.send(new ProducerRecord<>(topicName, "key-2", "value-2"));
        producer.flush();

        consumer.subscribe(List.of(topicName));

        // When
        final List<ConsumerRecord<String, String>> received = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + 10_000;
        while (received.size() < 2 && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(100)).forEach(received::add);
        }

        // Then
        assertThat(received).hasSize(2);
        assertThat(received.get(0).key()).isEqualTo("key-1");
        assertThat(received.get(0).value()).isEqualTo("value-1");
        assertThat(received.get(0).topic()).isEqualTo(topicName);
        assertThat(received.get(1).key()).isEqualTo("key-2");
        assertThat(received.get(1).value()).isEqualTo("value-2");
        assertThat(received.get(1).topic()).isEqualTo(topicName);

        log.info("Consumed {} messages successfully", received.size());
    }

    /**
     * Verifies that messages published with the same key (and therefore routed to the
     * same partition) are consumed in the order they were produced.
     *
     * @throws ExecutionException   if any producer future fails
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    @Test
    void shouldMaintainMessageOrderWithinPartition() throws ExecutionException, InterruptedException {
        // Given – same key forces all messages to the same partition
        final String commonKey = "same-key";
        final int messageCount = 10;

        // When – publish synchronously to guarantee ordering at the producer
        for (int i = 0; i < messageCount; i++) {
            producer.send(new ProducerRecord<>(topicName, commonKey, "message-" + i)).get();
        }

        consumer.subscribe(List.of(topicName));

        final List<String> receivedValues = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + 10_000;
        while (receivedValues.size() < messageCount && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(100)).forEach(r -> receivedValues.add(r.value()));
        }

        // Then
        assertThat(receivedValues).hasSize(messageCount);
        for (int i = 0; i < messageCount; i++) {
            assertThat(receivedValues.get(i)).isEqualTo("message-" + i);
        }

        log.info("Verified ordering for {} messages", messageCount);
    }

    /**
     * Verifies that a single record header is preserved faithfully through the
     * produce → store → fetch round-trip.
     *
     * <p>Headers are commonly used to carry distributed-tracing context (e.g.
     * W3C TraceContext, B3) and schema-registry magic bytes. A consumer must
     * receive the exact same header key and value bytes that the producer sent.</p>
     */
    @Test
    void shouldPreserveSingleHeaderThroughRoundTrip() {
        // Given
        final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "key", "value");
        record.headers().add("trace-id", "abc-123".getBytes(StandardCharsets.UTF_8));

        producer.send(record);
        producer.flush();

        consumer.subscribe(List.of(topicName));

        // When
        final List<ConsumerRecord<String, String>> received = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + 10_000;
        while (received.isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(100)).forEach(received::add);
        }

        // Then
        assertThat(received).hasSize(1);
        final Header traceHeader = received.get(0).headers().lastHeader("trace-id");
        assertThat(traceHeader).isNotNull();
        assertThat(new String(traceHeader.value(), StandardCharsets.UTF_8)).isEqualTo("abc-123");

        log.info("Single header preserved: key={}, value={}", traceHeader.key(),
            new String(traceHeader.value(), StandardCharsets.UTF_8));
    }

    /**
     * Verifies that multiple record headers are all preserved faithfully through the
     * produce → store → fetch round-trip, and that no headers are lost or duplicated.
     */
    @Test
    void shouldPreserveMultipleHeadersThroughRoundTrip() {
        // Given
        final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "key", "value");
        record.headers().add("content-type", "application/json".getBytes(StandardCharsets.UTF_8));
        record.headers().add("correlation-id", "corr-456".getBytes(StandardCharsets.UTF_8));
        record.headers().add("source-service", "order-service".getBytes(StandardCharsets.UTF_8));

        producer.send(record);
        producer.flush();

        consumer.subscribe(List.of(topicName));

        // When
        final List<ConsumerRecord<String, String>> received = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + 10_000;
        while (received.isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(100)).forEach(received::add);
        }

        // Then
        assertThat(received).hasSize(1);
        final Header[] headers = received.get(0).headers().toArray();
        assertThat(headers).hasSize(3);

        assertThat(new String(received.get(0).headers().lastHeader("content-type").value(), StandardCharsets.UTF_8))
            .isEqualTo("application/json");
        assertThat(new String(received.get(0).headers().lastHeader("correlation-id").value(), StandardCharsets.UTF_8))
            .isEqualTo("corr-456");
        assertThat(new String(received.get(0).headers().lastHeader("source-service").value(), StandardCharsets.UTF_8))
            .isEqualTo("order-service");

        log.info("All {} headers preserved correctly", headers.length);
    }

    /**
     * Creates a {@link KafkaProducer} configured for synchronous, at-least-once delivery.
     *
     * @param bootstrapServers the broker address
     * @return a new producer instance
     */
    private KafkaProducer<String, String> createProducer(final String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        return new KafkaProducer<>(props);
    }

    /**
     * Creates a {@link KafkaConsumer} configured to read from the earliest available offset.
     *
     * @param bootstrapServers the broker address
     * @return a new consumer instance
     */
    private KafkaConsumer<String, String> createConsumer(final String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(props);
    }
}
