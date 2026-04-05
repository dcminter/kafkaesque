package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Abstract base class defining integration tests for idempotent producer behaviour.
 *
 * <p>Covers two scenarios:</p>
 * <ol>
 *   <li><strong>Baseline:</strong> an idempotent producer can successfully publish records
 *       and every record is received exactly once under normal (no-failure) conditions.</li>
 *   <li><strong>Retry deduplication:</strong> when the broker's acknowledgement is lost in
 *       transit and the producer retries the same record batch (with the same producer ID,
 *       epoch, and base sequence number), the broker must store the record only once. A
 *       broker that lacks server-side idempotency support will store two copies, causing this
 *       test to fail.</li>
 * </ol>
 *
 * <p>Test 2 uses a {@link DropAckProxy} to intercept traffic between the producer and the
 * broker. The proxy rewrites {@code METADATA} responses to advertise its own address (so
 * that all subsequent client connections flow through it) and then silently drops the
 * broker's response to the first {@code PRODUCE} request. This forces the Kafka client to
 * time out and retry with the same sequence number — the only reliable way to trigger
 * server-side deduplication behaviour using a standard Kafka client.</p>
 *
 * <p>Subclasses must implement {@link #getBootstrapServers()} to supply the address of the
 * backend under test.</p>
 *
 * @see KafkaesqueIT
 * @see RealKafkaIT
 */
@Slf4j
abstract class AbstractIdempotentProducerBehaviorIT {

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
     * Creates a fresh, unique topic name for the current test.
     */
    @BeforeEach
    void setUp() {
        topicName = "test-idempotent-" + UUID.randomUUID();
    }

    /**
     * Verifies that an idempotent producer can publish records successfully under normal
     * conditions and that a consumer receives all records exactly once.
     *
     * <p>This test serves as a baseline: it confirms that the idempotent producer
     * initialisation handshake ({@code INIT_PRODUCER_ID}) and the produce/consume round-trip
     * both work correctly on the backend under test. It is expected to pass on both real
     * Kafka and Kafkaesque.</p>
     *
     * @throws Exception if producing or consuming fails
     */
    @Test
    void shouldSuccessfullyPublishRecordsWithIdempotentProducer() throws Exception {
        // Given - an idempotent producer connected directly to the broker
        try (var producer = KafkaTestClientFactory.createIdempotentProducer(getBootstrapServers())) {
            // When - five records are sent synchronously
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(topicName, "key-" + i, "value-" + i)).get();
            }
        }

        // Then - all five records are received by the consumer
        try (var consumer = KafkaTestClientFactory.createConsumer(getBootstrapServers())) {
            consumer.subscribe(List.of(topicName));

            final List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(received::add);
                return received.size() >= 5;
            });

            assertThat(received).hasSize(5);
            log.info("shouldSuccessfullyPublishRecordsWithIdempotentProducer: received {} records", received.size());
        }
    }

    /**
     * Verifies that the broker stores a record exactly once when the producer retries after
     * losing an acknowledgement.
     *
     * <p>A {@link DropAckProxy} is placed between the producer and the real broker. The proxy
     * rewrites {@code METADATA} responses so that the client routes all subsequent connections
     * through it, then drops the broker response to the first {@code PRODUCE} request. The
     * client times out (after {@code request.timeout.ms}) and retries the same batch with the
     * same producer ID, epoch, and base sequence number.</p>
     *
     * <p>An idempotency-aware broker (real Kafka) recognises the duplicate sequence number and
     * discards the second write, so the consumer sees exactly one record. A broker without
     * server-side idempotency support (current Kafkaesque) stores both writes, so the consumer
     * sees two records and this assertion fails — revealing the implementation gap.</p>
     *
     * @throws Exception if the proxy cannot be created, or if producing or consuming fails
     */
    @Test
    void shouldProduceExactlyOnceWhenAcknowledgementIsLost() throws Exception {
        // Given - a proxy that will drop the first PRODUCE response to force a client retry
        try (var proxy = new DropAckProxy(getBootstrapServers())) {

            // Producer points at the proxy, not directly at the broker
            try (var producer = KafkaTestClientFactory.createIdempotentProducer(proxy.getBootstrapServers())) {

                // When - arm the drop, then send; the ack is lost on the first attempt,
                //        the producer retries with the same (producerId, epoch, baseSequence)
                proxy.armDropNextProduceResponse();
                producer.send(new ProducerRecord<>(topicName, "key-1", "value-1")).get();

                log.info("shouldProduceExactlyOnceWhenAcknowledgementIsLost: send completed after retry");
            }
        }

        // Then - consume directly from the real broker and verify exactly one record
        //        (two records here means the broker stored both the original and the retry)
        try (var consumer = KafkaTestClientFactory.createConsumer(getBootstrapServers())) {
            consumer.subscribe(List.of(topicName));

            final List<ConsumerRecord<String, String>> received = new ArrayList<>();

            // Wait until the record arrives (the send already completed, so it should be immediate)
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(received::add);
                return received.size() >= 1;
            });

            // Poll a few extra times: if the broker stored a duplicate it will appear here
            consumer.poll(Duration.ofMillis(300)).forEach(received::add);
            consumer.poll(Duration.ofMillis(300)).forEach(received::add);

            assertThat(received)
                .as("broker must store the record exactly once even when the producer retries "
                    + "with the same sequence number")
                .hasSize(1);
            assertThat(received.get(0).value()).isEqualTo("value-1");

            log.info("shouldProduceExactlyOnceWhenAcknowledgementIsLost: consumer received {} record(s)",
                received.size());
        }
    }

    /**
     * Verifies that an idempotent producer tracks sequence numbers independently per
     * partition.
     *
     * <p>Each {@code (producerId, epoch, partition)} tuple maintains its own sequence
     * counter, so {@code baseSequence=0} is valid on partition 1 even after partition 0
     * has already advanced through several sequences. This test produces 3 records to
     * partition 0 and 2 records to partition 1, then asserts that all 5 arrive with the
     * correct per-partition distribution.</p>
     *
     * @throws Exception if topic creation, producing, or consuming fails
     */
    @Test
    void shouldTrackSequencesIndependentlyPerPartition() throws Exception {
        final var bootstrapServers = getBootstrapServers();

        // Create a two-partition topic explicitly so we can route to specific partitions
        final var adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (var admin = AdminClient.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic(topicName, 2, (short) 1))).all().get();
        }

        // Produce 3 records to partition 0 and 2 records to partition 1
        try (var producer = KafkaTestClientFactory.createIdempotentProducer(bootstrapServers)) {
            for (int i = 0; i < 3; i++) {
                producer.send(new ProducerRecord<>(topicName, 0, "key-p0-" + i, "val-p0-" + i)).get();
            }
            for (int i = 0; i < 2; i++) {
                producer.send(new ProducerRecord<>(topicName, 1, "key-p1-" + i, "val-p1-" + i)).get();
            }
        }

        // Consume all records and verify per-partition distribution
        try (var consumer = KafkaTestClientFactory.createConsumer(bootstrapServers)) {
            consumer.subscribe(List.of(topicName));

            final List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(received::add);
                return received.size() >= 5;
            });

            assertThat(received).hasSize(5);
            assertThat(received.stream().filter(r -> r.partition() == 0).count())
                .as("partition 0 must have exactly 3 records")
                .isEqualTo(3L);
            assertThat(received.stream().filter(r -> r.partition() == 1).count())
                .as("partition 1 must have exactly 2 records")
                .isEqualTo(2L);

            log.info("shouldTrackSequencesIndependentlyPerPartition: received {} record(s)", received.size());
        }
    }
}
