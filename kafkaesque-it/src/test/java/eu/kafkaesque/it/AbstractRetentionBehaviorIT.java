package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static eu.kafkaesque.it.KafkaCompat.poll;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.awaitility.Awaitility.await;

/**
 * Abstract base class defining retention-deletion behaviour tests.
 *
 * <p>Each test creates a topic via {@link AdminClient} with {@code cleanup.policy=delete}
 * and a short {@code retention.ms} window, then produces records whose timestamps already
 * fall outside that window.</p>
 *
 * <p>A short sleep followed by a current-timestamp "trigger" record is used to close the
 * segment containing the expired records, making them eligible for deletion on a real
 * broker. The broker-level retention check interval must be pre-configured on the real
 * broker (see {@code RealKafkaIT}) so that deletion occurs promptly. The assertion then
 * checks that the expired keys are absent from the log.</p>
 *
 * <p>Kafkaesque filters expired records immediately at fetch time, so the condition is
 * satisfied on the first poll without any background deletion.</p>
 *
 * <p>Subclasses must implement {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractRetentionBehaviorIT {

    /** Topic-level segment age that triggers a roll (ms). */
    private static final int SEGMENT_ROLL_MS = 1_000;

    /** Sleep duration to ensure the segment has aged past {@link #SEGMENT_ROLL_MS}. */
    private static final int SEGMENT_ROLL_SLEEP_MS = SEGMENT_ROLL_MS + 200;

    /** How far in the past the expired records are timestamped (ms). */
    private static final long EXPIRED_OFFSET_MS = 10_000L;

    /** Retention window for the test topic (ms); expired records predate this. */
    private static final long RETENTION_MS = 5_000L;

    /** Byte budget for the retention.bytes test; chosen so that a fraction of the produced records survive. */
    private static final int RETENTION_BYTES = 1024;

    /** Length of the large value string produced in the retention.bytes test. */
    private static final int LARGE_VALUE_LENGTH = 300;

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Verifies that records whose timestamps predate the topic's {@code retention.ms}
     * window are not returned to a consumer, while records within the window are.
     *
     * <p>Records are produced with a timestamp {@value #EXPIRED_OFFSET_MS} ms in the past,
     * which is beyond {@code retention.ms=}{@value #RETENTION_MS} ms, making them
     * immediately eligible for deletion. A trigger record with the current timestamp
     * closes the expired segment and serves as a live record to confirm the consumer
     * is working.</p>
     *
     * @throws Exception if the admin client, producer, or consumer operation fails
     */
    @Test
    void shouldNotReturnRecordsExpiredByRetentionMs() throws Exception {
        final String bootstrapServers = getBootstrapServers();
        final String topicName = "retention-ms-test-" + UUID.randomUUID();

        createRetentionTestTopic(bootstrapServers, topicName);

        // Produce records with timestamps already outside the retention window
        final long expiredTimestamp = System.currentTimeMillis() - EXPIRED_OFFSET_MS;
        try (final KafkaProducer<String, String> producer =
                KafkaTestClientFactory.createProducer(bootstrapServers)) {
            producer.send(new ProducerRecord<>(topicName, 0, expiredTimestamp, "k1", "value-1")).get();
            producer.send(new ProducerRecord<>(topicName, 0, expiredTimestamp, "k2", "value-2")).get();
            producer.flush();
            // Wait for segment.ms to elapse, then close the expired segment by appending a
            // current-timestamp trigger record.  The expired segment is then eligible for
            // deletion once the broker's retention check fires.
            Thread.sleep(SEGMENT_ROLL_SLEEP_MS);
            producer.send(new ProducerRecord<>(topicName, "retention-trigger", "live")).get();
            producer.flush();
        }

        // Wait until the expired keys (k1, k2) are no longer returned to a consumer.
        // The trigger record ("retention-trigger") is within the retention window and
        // may still appear; we only assert the absence of the expired keys.
        try (final KafkaConsumer<String, String> consumer =
                KafkaTestClientFactory.createConsumer(bootstrapServers)) {
            consumer.subscribe(List.of(topicName));

            // Obtain partition assignment before seeking
            await().atMost(10, SECONDS).until(() -> {
                poll(consumer, 200);
                return !consumer.assignment().isEmpty();
            });

            await().atMost(60, SECONDS).pollInterval(2, SECONDS).until(() -> {
                consumer.seekToBeginning(consumer.assignment());
                final List<ConsumerRecord<String, String>> received = new ArrayList<>();
                int emptyPolls = 0;
                while (emptyPolls < 3) {
                    final var batch = poll(consumer, 300);
                    if (batch.isEmpty()) {
                        emptyPolls++;
                    } else {
                        emptyPolls = 0;
                        batch.forEach(received::add);
                    }
                }
                return received.stream().noneMatch(r -> "k1".equals(r.key()) || "k2".equals(r.key()));
            });
        }

        log.info("Retention verified: expired records not returned from {}", topicName);
    }

    /**
     * Verifies that records are dropped when the partition log exceeds the topic's
     * {@code retention.bytes} limit, with the oldest records removed first.
     *
     * <p>Ten large records (each roughly {@value #LARGE_VALUE_LENGTH} bytes of value payload)
     * are produced to a topic whose byte budget is {@value #RETENTION_BYTES} bytes.  This far
     * exceeds the budget, so the oldest records must be purged.  The assertion checks that the
     * first-produced record ({@code key-0}) is no longer visible to a consumer reading from the
     * beginning of the partition.</p>
     *
     * <p>Kafkaesque filters by bytes eagerly at fetch time.  On a real broker the log cleaner
     * deletes segments once the configured check interval fires (shortened to 1 second via
     * {@link #configureShortRetentionCheckInterval}), so Awaitility is used to retry until
     * the condition is satisfied.</p>
     *
     * @throws Exception if the admin client, producer, or consumer operation fails
     */
    @Test
    void shouldNotReturnRecordsExceedingRetentionBytes() throws Exception {
        final String bootstrapServers = getBootstrapServers();
        final String topicName = "retention-bytes-test-" + UUID.randomUUID();

        createRetentionBytesTestTopic(bootstrapServers, topicName);

        // Each record is well above RETENTION_BYTES / 10, so after 10 records the
        // combined log size is many times the budget and the oldest records must be purged.
        final String largeValue = "X".repeat(LARGE_VALUE_LENGTH);
        try (final KafkaProducer<String, String> producer =
                KafkaTestClientFactory.createProducer(bootstrapServers)) {
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>(topicName, "key-" + i, largeValue)).get();
            }
            producer.flush();
            // Allow the active segment to age past segment.ms so the log cleaner can act on it.
            Thread.sleep(SEGMENT_ROLL_SLEEP_MS);
            producer.send(new ProducerRecord<>(topicName, "live-key", "live-value")).get();
            producer.flush();
        }

        try (final KafkaConsumer<String, String> consumer =
                KafkaTestClientFactory.createConsumer(bootstrapServers)) {
            consumer.subscribe(List.of(topicName));

            await().atMost(10, SECONDS).until(() -> {
                poll(consumer, 200);
                return !consumer.assignment().isEmpty();
            });

            // Wait until the oldest record (key-0) is no longer returned when reading from the start.
            await().atMost(60, SECONDS).pollInterval(2, SECONDS).until(() -> {
                consumer.seekToBeginning(consumer.assignment());
                final List<ConsumerRecord<String, String>> received = new ArrayList<>();
                int emptyPolls = 0;
                while (emptyPolls < 3) {
                    final var batch = poll(consumer, 300);
                    if (batch.isEmpty()) {
                        emptyPolls++;
                    } else {
                        emptyPolls = 0;
                        batch.forEach(received::add);
                    }
                }
                return received.stream().noneMatch(r -> "key-0".equals(r.key()));
            });
        }

        log.info("Byte retention verified: oldest records not returned from {}", topicName);
    }

    /**
     * Creates a topic configured for retention.bytes testing with a small segment size
     * to ensure segments roll frequently, making old data eligible for deletion promptly.
     *
     * @param bootstrapServers the broker address
     * @param topicName        the topic to create
     * @throws Exception if topic creation fails
     */
    private void createRetentionBytesTestTopic(final String bootstrapServers, final String topicName)
            throws Exception {
        try (final AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000))) {
            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1)
                .configs(Map.of(
                    "cleanup.policy", "delete",
                    "retention.bytes", String.valueOf(RETENTION_BYTES),
                    "segment.bytes", "512",
                    "segment.ms", String.valueOf(SEGMENT_ROLL_MS)
                )))).all().get();
        }
    }

    /**
     * Creates a topic configured for retention-deletion testing.
     *
     * @param bootstrapServers the broker address
     * @param topicName        the topic to create
     * @throws Exception if topic creation fails
     */
    private void createRetentionTestTopic(final String bootstrapServers, final String topicName)
            throws Exception {
        try (final AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000))) {
            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1)
                .configs(Map.of(
                    "cleanup.policy", "delete",
                    "retention.ms", String.valueOf(RETENTION_MS),
                    "segment.ms", String.valueOf(SEGMENT_ROLL_MS)
                )))).all().get();
        }
    }

}
