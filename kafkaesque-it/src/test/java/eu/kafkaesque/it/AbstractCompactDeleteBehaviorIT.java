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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Abstract base class defining combined compact-and-delete behaviour tests.
 *
 * <p>Each test creates a topic via {@link AdminClient} with
 * {@code cleanup.policy=compact,delete} and verifies that both compaction
 * (deduplication by key, tombstone removal) and retention (time-based or
 * byte-based) are applied to fetch responses.</p>
 *
 * <p>Aggressive compaction settings are used so that compaction occurs quickly
 * on a real broker. A short sleep followed by a segment-roll trigger record is
 * used to ensure the active segment closes before the log cleaner runs.</p>
 *
 * <p>Subclasses must implement {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractCompactDeleteBehaviorIT {

    /** Topic-level segment age that triggers a roll (ms). */
    private static final int SEGMENT_ROLL_MS = 1_000;

    /** Sleep duration to ensure the segment has aged past {@link #SEGMENT_ROLL_MS}. */
    private static final int SEGMENT_ROLL_SLEEP_MS = SEGMENT_ROLL_MS + 200;

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Verifies that compaction (keeping only the latest value per key) is applied
     * when the cleanup policy is {@code compact,delete}.
     *
     * @throws Exception if the admin client, producer, or consumer operation fails
     */
    @Test
    void shouldApplyCompactionForCompactDeleteTopic() throws Exception {
        final String bootstrapServers = getBootstrapServers();
        final String topicName = "compact-delete-compaction-" + UUID.randomUUID();

        createCompactDeleteTopic(bootstrapServers, topicName);

        try (final KafkaProducer<String, String> producer =
                KafkaTestClientFactory.createProducer(bootstrapServers)) {
            producer.send(new ProducerRecord<>(topicName, "k1", "value-1")).get();
            producer.send(new ProducerRecord<>(topicName, "k1", "value-2")).get();
            producer.send(new ProducerRecord<>(topicName, "k1", "value-3")).get();
            producer.send(new ProducerRecord<>(topicName, "k2", "value-a")).get();
            producer.flush();
            Thread.sleep(SEGMENT_ROLL_SLEEP_MS);
            producer.send(new ProducerRecord<>(topicName, "segment-roll-trigger", "")).get();
            producer.flush();
        }

        try (final KafkaConsumer<String, String> consumer =
                KafkaTestClientFactory.createConsumer(bootstrapServers)) {
            consumer.subscribe(List.of(topicName));

            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200));
                return !consumer.assignment().isEmpty();
            });

            await().atMost(30, SECONDS).pollInterval(2, SECONDS).until(() -> {
                consumer.seekToBeginning(consumer.assignment());
                final List<ConsumerRecord<String, String>> records = new ArrayList<>();
                int emptyPolls = 0;
                while (emptyPolls < 3) {
                    final var batch = consumer.poll(Duration.ofMillis(300));
                    if (batch.isEmpty()) {
                        emptyPolls++;
                    } else {
                        emptyPolls = 0;
                        batch.forEach(records::add);
                    }
                }
                final var k1Records = records.stream()
                    .filter(r -> "k1".equals(r.key()))
                    .toList();
                return k1Records.size() == 1 && "value-3".equals(k1Records.get(0).value());
            });

            log.info("Compaction verified for compact,delete topic: only latest record per key retained in {}",
                topicName);
        }
    }

    /**
     * Verifies that time-based retention is applied after compaction when the cleanup
     * policy is {@code compact,delete}.
     *
     * <p>Records that survive compaction but fall outside the retention window must
     * be removed from fetch responses.</p>
     *
     * @throws Exception if the admin client, producer, or consumer operation fails
     */
    @Test
    void shouldApplyRetentionMsAfterCompactionForCompactDeleteTopic() throws Exception {
        final String bootstrapServers = getBootstrapServers();
        final String topicName = "compact-delete-retention-ms-" + UUID.randomUUID();

        createCompactDeleteTopicWithRetentionMs(bootstrapServers, topicName, 5_000L);

        final long expiredTimestamp = System.currentTimeMillis() - 10_000L;

        try (final KafkaProducer<String, String> producer =
                KafkaTestClientFactory.createProducer(bootstrapServers)) {
            // k1: only an expired record — survives compaction but must be dropped by retention.ms
            producer.send(new ProducerRecord<>(topicName, 0, expiredTimestamp, "k1", "old-value")).get();
            // k2: a live record — must survive both compaction and retention
            producer.send(new ProducerRecord<>(topicName, "k2", "live-value")).get();
            producer.flush();
            Thread.sleep(SEGMENT_ROLL_SLEEP_MS);
            producer.send(new ProducerRecord<>(topicName, "segment-roll-trigger", "")).get();
            producer.flush();
        }

        try (final KafkaConsumer<String, String> consumer =
                KafkaTestClientFactory.createConsumer(bootstrapServers)) {
            consumer.subscribe(List.of(topicName));

            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200));
                return !consumer.assignment().isEmpty();
            });

            await().atMost(30, SECONDS).pollInterval(2, SECONDS).until(() -> {
                consumer.seekToBeginning(consumer.assignment());
                final List<ConsumerRecord<String, String>> records = new ArrayList<>();
                int emptyPolls = 0;
                while (emptyPolls < 3) {
                    final var batch = consumer.poll(Duration.ofMillis(300));
                    if (batch.isEmpty()) {
                        emptyPolls++;
                    } else {
                        emptyPolls = 0;
                        batch.forEach(records::add);
                    }
                }
                final boolean k1Absent = records.stream().noneMatch(r -> "k1".equals(r.key()));
                final var k2Records = records.stream().filter(r -> "k2".equals(r.key())).toList();
                return k1Absent && k2Records.size() == 1 && "live-value".equals(k2Records.get(0).value());
            });

            log.info("Retention verified for compact,delete topic: expired records removed in {}",
                topicName);
        }
    }

    /**
     * Creates a topic with {@code cleanup.policy=compact,delete} and aggressive compaction
     * settings to minimise the delay before compaction runs on a real Kafka broker.
     *
     * @param bootstrapServers the broker address
     * @param topicName        the topic to create
     * @throws Exception if topic creation fails
     */
    private void createCompactDeleteTopic(final String bootstrapServers, final String topicName)
            throws Exception {
        try (final AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000))) {
            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1)
                .configs(Map.of(
                    "cleanup.policy", "compact,delete",
                    "min.cleanable.dirty.ratio", "0.01",
                    "min.compaction.lag.ms", "0",
                    "delete.retention.ms", "0",
                    "segment.ms", String.valueOf(SEGMENT_ROLL_MS)
                )))).all().get();
        }
    }

    /**
     * Creates a topic with {@code cleanup.policy=compact,delete}, aggressive compaction
     * settings, and a specific {@code retention.ms} value.
     *
     * @param bootstrapServers the broker address
     * @param topicName        the topic to create
     * @param retentionMs      the retention period in milliseconds
     * @throws Exception if topic creation fails
     */
    private void createCompactDeleteTopicWithRetentionMs(
            final String bootstrapServers, final String topicName, final long retentionMs)
            throws Exception {
        try (final AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000))) {
            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1)
                .configs(Map.of(
                    "cleanup.policy", "compact,delete",
                    "min.cleanable.dirty.ratio", "0.01",
                    "min.compaction.lag.ms", "0",
                    "delete.retention.ms", "0",
                    "segment.ms", String.valueOf(SEGMENT_ROLL_MS),
                    "retention.ms", String.valueOf(retentionMs)
                )))).all().get();
        }
    }
}
