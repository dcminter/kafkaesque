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
import java.util.stream.Collectors;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.awaitility.Awaitility.await;

/**
 * Abstract base class defining log-compaction behaviour tests.
 *
 * <p>Each test creates a topic via {@link AdminClient} with {@code cleanup.policy=compact}
 * and aggressive compaction settings so that compaction occurs quickly on a real broker.
 * A short sleep followed by a segment-roll trigger record is used to ensure the
 * active segment closes before the log cleaner runs, making the test deterministic
 * on both Kafkaesque (immediate) and real Kafka (within the Awaitility window).</p>
 *
 * <p>Subclasses must implement {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractCompactionBehaviorIT {

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
     * Verifies that, for a compacted topic, a consumer starting from the beginning
     * eventually sees only the latest record for each key.
     *
     * @throws Exception if the admin client, producer, or consumer operation fails
     */
    @Test
    void shouldRetainOnlyLatestRecordPerKeyInCompactedTopic() throws Exception {
        final String bootstrapServers = getBootstrapServers();
        final String topicName = "compaction-latest-" + UUID.randomUUID();

        createCompactTopic(bootstrapServers, topicName);

        try (final KafkaProducer<String, String> producer =
                KafkaTestClientFactory.createProducer(bootstrapServers)) {
            producer.send(new ProducerRecord<>(topicName, "k1", "value-1")).get();
            producer.send(new ProducerRecord<>(topicName, "k1", "value-2")).get();
            producer.send(new ProducerRecord<>(topicName, "k1", "value-3")).get();
            producer.send(new ProducerRecord<>(topicName, "k2", "value-a")).get();
            producer.flush();
            // Wait for segment.ms to elapse, then trigger a roll by appending one more record.
            // This moves the records above into a closed segment that the log cleaner can compact.
            Thread.sleep(SEGMENT_ROLL_SLEEP_MS);
            producer.send(new ProducerRecord<>(topicName, "segment-roll-trigger", "")).get();
            producer.flush();
        }

        try (final KafkaConsumer<String, String> consumer =
                KafkaTestClientFactory.createConsumer(bootstrapServers)) {
            consumer.subscribe(List.of(topicName));

            // Obtain partition assignment before seeking
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200));
                return !consumer.assignment().isEmpty();
            });

            // Wait until compaction has reduced k1 to a single record with the latest value
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
                    .collect(Collectors.toList());
                return k1Records.size() == 1 && "value-3".equals(k1Records.get(0).value());
            });

            log.info("Compaction verified: only latest record per key retained in {}", topicName);
        }
    }

    /**
     * Verifies that a null-value record (tombstone) causes the non-null record for the
     * corresponding key to be absent from a compacted topic once one compaction pass has run.
     *
     * <p>After a single compaction pass the tombstone itself (null value) may still be visible
     * to consumers; only the original non-null record for the key must be gone. This makes the
     * assertion achievable in a single pass on a real broker.</p>
     *
     * @throws Exception if the admin client, producer, or consumer operation fails
     */
    @Test
    void shouldTreatNullValueAsTombstoneInCompactedTopic() throws Exception {
        final String bootstrapServers = getBootstrapServers();
        final String topicName = "compaction-tombstone-" + UUID.randomUUID();

        createCompactTopic(bootstrapServers, topicName);

        try (final KafkaProducer<String, String> producer =
                KafkaTestClientFactory.createProducer(bootstrapServers)) {
            producer.send(new ProducerRecord<>(topicName, "k1", "value-1")).get();
            producer.send(new ProducerRecord<>(topicName, "k1", null)).get(); // tombstone
            producer.flush();
            // Trigger segment roll so the log cleaner can compact the closed segment
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

            // Wait until the original record (k1=value-1) has been compacted away.
            // After one compaction pass the tombstone (k1=null) may still be present;
            // we only assert that no non-null k1 record remains so that a single pass suffices.
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
                return records.stream().noneMatch(r -> "k1".equals(r.key()) && r.value() != null);
            });

            log.info("Tombstone verified: non-null record for key absent after compaction in {}", topicName);
        }
    }

    /**
     * Creates a compacted topic with aggressive compaction settings to minimise the delay
     * before compaction runs on a real Kafka broker.
     *
     * @param bootstrapServers the broker address
     * @param topicName        the topic to create
     * @throws Exception if topic creation fails
     */
    private void createCompactTopic(final String bootstrapServers, final String topicName)
            throws Exception {
        try (final AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000))) {
            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1)
                .configs(Map.of(
                    "cleanup.policy", "compact",
                    "min.cleanable.dirty.ratio", "0.01",
                    "min.compaction.lag.ms", "0",
                    "delete.retention.ms", "0",
                    "segment.ms", String.valueOf(SEGMENT_ROLL_MS)
                )))).all().get();
        }
    }
}
