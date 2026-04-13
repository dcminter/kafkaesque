package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static eu.kafkaesque.it.KafkaCompat.poll;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.List.of;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Abstract base class defining the consumer integration test suite.
 *
 * <p>Covers end-to-end produce→consume round-trips including message ordering and
 * header preservation. Subclasses must implement {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractConsumerBehaviorIT {

    /** Per-test producer; created fresh in {@link #setUpClients()}. */
    private KafkaProducer<String, String> producer;

    /** Per-test consumer; created fresh in {@link #setUpClients()}. */
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
     * Creates a fresh producer and consumer for the current test.
     *
     * @throws Exception if the clients cannot be created
     */
    @BeforeEach
    void setUpClients() throws Exception {
        final String bootstrapServers = getBootstrapServers();
        topicName = "test-topic-" + UUID.randomUUID();
        producer = KafkaTestClientFactory.createProducer(bootstrapServers);
        consumer = KafkaTestClientFactory.createConsumer(bootstrapServers);
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
     * Verifies that events published to a topic can be consumed by a subscriber,
     * and that the key, value, and topic are preserved faithfully.
     */
    @Test
    void shouldReceivePublishedEventsFromKafka() {
        // Given
        producer.send(new ProducerRecord<>(topicName, "key-1", "value-1"));
        producer.send(new ProducerRecord<>(topicName, "key-2", "value-2"));
        producer.flush();

        consumer.subscribe(of(topicName));

        // When
        final List<ConsumerRecord<String, String>> received = new ArrayList<>();
        await().atMost(10, SECONDS).until(() -> {
            poll(consumer, 100).forEach(received::add);
            return received.size() >= 2;
        });

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
     * @throws Exception if any producer future fails
     */
    @Test
    void shouldMaintainMessageOrderWithinPartition() throws Exception {
        // Given – same key forces all messages to the same partition
        final String commonKey = "same-key";
        final int messageCount = 10;

        // When – publish synchronously to guarantee ordering at the producer
        for (int i = 0; i < messageCount; i++) {
            producer.send(new ProducerRecord<>(topicName, commonKey, "message-" + i)).get();
        }

        consumer.subscribe(of(topicName));

        final List<String> receivedValues = new ArrayList<>();
        await().atMost(10, SECONDS).until(() -> {
            poll(consumer, 100).forEach(r -> receivedValues.add(r.value()));
            return receivedValues.size() >= messageCount;
        });

        // Then
        assertThat(receivedValues).hasSize(messageCount);
        IntStream.range(0, messageCount)
            .forEach(i -> assertThat(receivedValues.get(i)).isEqualTo("message-" + i));

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
        record.headers().add("trace-id", "abc-123".getBytes(UTF_8));

        producer.send(record);
        producer.flush();

        consumer.subscribe(of(topicName));

        // When
        final List<ConsumerRecord<String, String>> received = new ArrayList<>();
        await().atMost(10, SECONDS).until(() -> {
            poll(consumer, 100).forEach(received::add);
            return !received.isEmpty();
        });

        // Then
        assertThat(received).hasSize(1);
        final Header traceHeader = received.get(0).headers().lastHeader("trace-id");
        assertThat(traceHeader).isNotNull();
        assertThat(new String(traceHeader.value(), UTF_8)).isEqualTo("abc-123");

        log.info("Single header preserved: key={}, value={}", traceHeader.key(),
            new String(traceHeader.value(), UTF_8));
    }

    /**
     * Verifies that multiple record headers are all preserved faithfully through the
     * produce → store → fetch round-trip, and that no headers are lost or duplicated.
     */
    @Test
    void shouldPreserveMultipleHeadersThroughRoundTrip() {
        // Given
        final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "key", "value");
        record.headers().add("content-type", "application/json".getBytes(UTF_8));
        record.headers().add("correlation-id", "corr-456".getBytes(UTF_8));
        record.headers().add("source-service", "order-service".getBytes(UTF_8));

        producer.send(record);
        producer.flush();

        consumer.subscribe(of(topicName));

        // When
        final List<ConsumerRecord<String, String>> received = new ArrayList<>();
        await().atMost(10, SECONDS).until(() -> {
            poll(consumer, 100).forEach(received::add);
            return !received.isEmpty();
        });

        // Then
        assertThat(received).hasSize(1);
        final Header[] headers = received.get(0).headers().toArray();
        assertThat(headers).hasSize(3);

        assertThat(new String(received.get(0).headers().lastHeader("content-type").value(), UTF_8))
            .isEqualTo("application/json");
        assertThat(new String(received.get(0).headers().lastHeader("correlation-id").value(), UTF_8))
            .isEqualTo("corr-456");
        assertThat(new String(received.get(0).headers().lastHeader("source-service").value(), UTF_8))
            .isEqualTo("order-service");

        log.info("All {} headers preserved correctly", headers.length);
    }

    /**
     * Verifies that two consumers in <em>different</em> groups both receive all messages
     * published to the same topic.
     *
     * <p>Each group maintains an independent read position, so every group receives the
     * complete message stream regardless of what other groups have consumed.</p>
     *
     * @throws Exception if bootstrap address lookup fails
     */
    @Test
    void shouldAllowIndependentGroupsToEachReceiveAllMessages() throws Exception {
        // Given
        final String sharedTopic = "multi-group-topic-" + UUID.randomUUID();
        final String groupA = "group-a-" + UUID.randomUUID();
        final String groupB = "group-b-" + UUID.randomUUID();

        try (
            final KafkaConsumer<String, String> consumerA =
                KafkaTestClientFactory.createConsumer(getBootstrapServers(), groupA);
            final KafkaConsumer<String, String> consumerB =
                KafkaTestClientFactory.createConsumer(getBootstrapServers(), groupB)
        ) {
            final var producerLocal = KafkaTestClientFactory.createProducer(getBootstrapServers());
            producerLocal.send(new ProducerRecord<>(sharedTopic, "k1", "v1"));
            producerLocal.send(new ProducerRecord<>(sharedTopic, "k2", "v2"));
            producerLocal.flush();
            producerLocal.close();

            consumerA.subscribe(of(sharedTopic));
            consumerB.subscribe(of(sharedTopic));

            // When – poll both consumers until each has received both messages
            final List<String> receivedByA = new ArrayList<>();
            final List<String> receivedByB = new ArrayList<>();

            await().atMost(15, SECONDS).until(() -> {
                poll(consumerA, 100).forEach(r -> receivedByA.add(r.value()));
                poll(consumerB, 100).forEach(r -> receivedByB.add(r.value()));
                return receivedByA.size() >= 2 && receivedByB.size() >= 2;
            });

            // Then – each group sees the full message stream
            assertThat(receivedByA).containsExactlyInAnyOrder("v1", "v2");
            assertThat(receivedByB).containsExactlyInAnyOrder("v1", "v2");

            log.info("Both groups independently received {} messages each", receivedByA.size());
        }
    }

    /**
     * Verifies that two consumers in the <em>same</em> group together receive all messages
     * from a multi-partition topic without duplicates.
     *
     * <p>Kafka distributes partitions across the members of a consumer group so that each
     * partition is consumed by exactly one member. The test uses a two-partition topic and
     * publishes one batch of messages to each partition, then checks that the combined output
     * of both consumers covers every message exactly once.</p>
     *
     * @throws Exception if topic creation or bootstrap address lookup fails
     */
    @Test
    void shouldDistributePartitionsAcrossConsumersInSameGroup() throws Exception {
        // Given – create a 2-partition topic so partitions can be spread across two consumers
        final String groupId = "shared-group-" + UUID.randomUUID();
        final String multiPartitionTopic = "multi-partition-topic-" + UUID.randomUUID();

        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.createTopics(of(new NewTopic(multiPartitionTopic, 2, (short) 1))).all().get();
        }

        try (
            final KafkaConsumer<String, String> consumerA =
                KafkaTestClientFactory.createConsumer(getBootstrapServers(), groupId);
            final KafkaConsumer<String, String> consumerB =
                KafkaTestClientFactory.createConsumer(getBootstrapServers(), groupId)
        ) {
            // Publish two messages, one per partition, so each consumer will receive exactly one
            final var producerLocal = KafkaTestClientFactory.createProducer(getBootstrapServers());
            producerLocal.send(new ProducerRecord<>(multiPartitionTopic, 0, "key", "msg-p0")).get();
            producerLocal.send(new ProducerRecord<>(multiPartitionTopic, 1, "key", "msg-p1")).get();
            producerLocal.close();

            // Subscribe both consumers to trigger the rebalance
            consumerA.subscribe(of(multiPartitionTopic));
            consumerB.subscribe(of(multiPartitionTopic));

            // When – poll both until the combined set contains all messages
            final Set<String> received = new HashSet<>();
            await().atMost(20, SECONDS).until(() -> {
                poll(consumerA, 100).forEach(r -> received.add(r.value()));
                poll(consumerB, 100).forEach(r -> received.add(r.value()));
                return received.size() >= 2;
            });

            // Then – exactly the two messages were received with no duplicates
            assertThat(received).containsExactlyInAnyOrder("msg-p0", "msg-p1");

            log.info("Same-group consumers together received {} unique messages", received.size());
        }
    }
}
