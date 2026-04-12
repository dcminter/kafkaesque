package eu.kafkaesque.core;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.List.copyOf;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for log-compaction and retention (time and bytes) filtering in
 * {@link ConsumerDataApiHandler}, exercised end-to-end via the full Kafka client
 * stack against an in-process {@link KafkaesqueServer}.
 *
 * <p>Each test creates a fresh topic with specific cleanup policies, publishes records,
 * then consumes them and asserts on the filtered result.  Because Kafkaesque applies
 * all policies at fetch time the assertions hold on the first successful poll without
 * any Awaitility polling.</p>
 */
class TopicPolicyFilterTest {

    private KafkaesqueServer server;
    private AdminClient adminClient;
    private KafkaProducer<String, String> producer;

    @BeforeEach
    void setUp() throws IOException {
        server = new KafkaesqueServer("localhost", 0);
        server.start();

        final var adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server.getBootstrapServers());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        adminClient = AdminClient.create(adminProps);

        final var producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        producer = new KafkaProducer<>(producerProps);
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
        if (server != null) {
            server.close();
        }
    }

    // -------------------------------------------------------------------------
    // Compaction tests
    // -------------------------------------------------------------------------

    @Test
    void shouldRetainOnlyLatestValuePerKeyAfterCompaction() throws Exception {
        final var topic = "compact-latest-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "compact"));

        producer.send(new ProducerRecord<>(topic, "k1", "v1")).get();
        producer.send(new ProducerRecord<>(topic, "k1", "v2")).get();
        producer.send(new ProducerRecord<>(topic, "k1", "v3")).get();
        producer.send(new ProducerRecord<>(topic, "k2", "va")).get();
        producer.flush();

        final var records = drainRecords(topic);

        final var k1Records = records.stream().filter(r -> "k1".equals(r.key())).collect(Collectors.toList());
        final var k2Records = records.stream().filter(r -> "k2".equals(r.key())).collect(Collectors.toList());
        assertThat(k1Records).hasSize(1);
        assertThat(k1Records.get(0).value()).isEqualTo("v3");
        assertThat(k2Records).hasSize(1);
        assertThat(k2Records.get(0).value()).isEqualTo("va");
    }

    @Test
    void shouldRemoveTombstonedKeyAfterCompaction() throws Exception {
        final var topic = "compact-tombstone-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "compact"));

        producer.send(new ProducerRecord<>(topic, "k1", "v1")).get();
        producer.send(new ProducerRecord<>(topic, "k1", (String) null)).get(); // tombstone
        producer.flush();

        final var records = drainRecords(topic);

        // Kafkaesque eagerly removes tombstoned keys at fetch time
        assertThat(records.stream().filter(r -> "k1".equals(r.key())).collect(Collectors.toList())).isEmpty();
    }

    @Test
    void shouldRetainNullKeyRecordsAfterCompaction() throws Exception {
        final var topic = "compact-null-key-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "compact"));

        producer.send(new ProducerRecord<>(topic, (String) null, "null-rec-1")).get();
        producer.send(new ProducerRecord<>(topic, "k1", "v1")).get();
        producer.send(new ProducerRecord<>(topic, (String) null, "null-rec-2")).get();
        producer.send(new ProducerRecord<>(topic, "k1", "v2")).get();
        producer.flush();

        final var records = drainRecords(topic);

        // Null-key records are always retained; only the latest k1 survives
        assertThat(records.stream().filter(r -> r.key() == null).collect(Collectors.toList())).hasSize(2);
        final var k1Records = records.stream().filter(r -> "k1".equals(r.key())).collect(Collectors.toList());
        assertThat(k1Records).hasSize(1);
        assertThat(k1Records.get(0).value()).isEqualTo("v2");
    }

    // -------------------------------------------------------------------------
    // RetentionMs tests
    // -------------------------------------------------------------------------

    @Test
    void shouldFilterRecordsOlderThanRetentionMs() throws Exception {
        final var topic = "retention-ms-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "delete", "retention.ms", "5000"));

        final long expiredTimestamp = System.currentTimeMillis() - 10_000L;
        producer.send(new ProducerRecord<>(topic, 0, expiredTimestamp, "k1", "old-1")).get();
        producer.send(new ProducerRecord<>(topic, 0, expiredTimestamp, "k2", "old-2")).get();
        producer.send(new ProducerRecord<>(topic, "live-key", "live-value")).get();
        producer.flush();

        final var records = drainRecords(topic);

        assertThat(records.stream().filter(r -> "k1".equals(r.key())).collect(Collectors.toList())).isEmpty();
        assertThat(records.stream().filter(r -> "k2".equals(r.key())).collect(Collectors.toList())).isEmpty();
        assertThat(records.stream().filter(r -> "live-key".equals(r.key())).collect(Collectors.toList())).hasSize(1);
    }

    // -------------------------------------------------------------------------
    // RetentionBytes tests
    // -------------------------------------------------------------------------

    @Test
    void shouldRetainOnlyMostRecentRecordsWithinBytesBudget() throws Exception {
        // Each record: key 2 bytes + value 3 bytes = 5 bytes.
        // Budget 10: newest k3(5) + k2(5) = 10 <= 10, included; k1 would total 15 > 10, dropped.
        final var topic = "retention-bytes-partial-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "delete", "retention.bytes", "10"));

        producer.send(new ProducerRecord<>(topic, "k1", "abc")).get();
        producer.send(new ProducerRecord<>(topic, "k2", "xyz")).get();
        producer.send(new ProducerRecord<>(topic, "k3", "pqr")).get();
        producer.flush();

        final var records = drainRecords(topic);

        assertThat(records.stream().filter(r -> "k1".equals(r.key())).collect(Collectors.toList())).isEmpty();
        assertThat(records.stream().filter(r -> "k2".equals(r.key())).collect(Collectors.toList())).hasSize(1);
        assertThat(records.stream().filter(r -> "k3".equals(r.key())).collect(Collectors.toList())).hasSize(1);
    }

    @Test
    void shouldRetainAllRecordsWhenByteBudgetNotExceeded() throws Exception {
        final var topic = "retention-bytes-all-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "delete", "retention.bytes", "100"));

        producer.send(new ProducerRecord<>(topic, "k1", "v1")).get();
        producer.send(new ProducerRecord<>(topic, "k2", "v2")).get();
        producer.send(new ProducerRecord<>(topic, "k3", "v3")).get();
        producer.flush();

        final var records = drainRecords(topic);

        assertThat(records).hasSize(3);
    }

    @Test
    void shouldRetainNoRecordsWhenSingleRecordExceedsBudget() throws Exception {
        // (k1,v1) = 2+2 = 4 bytes > budget of 1; takeWhile stops immediately
        final var topic = "retention-bytes-none-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "delete", "retention.bytes", "1"));

        producer.send(new ProducerRecord<>(topic, "k1", "v1")).get();
        producer.flush();

        final var records = drainRecords(topic);

        assertThat(records).isEmpty();
    }

    @Test
    void shouldHandleNullKeyInByteEstimation() throws Exception {
        // (null,"abc") = 0+3=3 bytes; (null,"defg") = 0+4=4 bytes.
        // Budget 5: newest (defg, 4B) accumulates 4<=5 → include; next (abc) accumulates 7>5 → drop.
        final var topic = "retention-bytes-null-key-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "delete", "retention.bytes", "5"));

        producer.send(new ProducerRecord<>(topic, (String) null, "abc")).get();
        producer.send(new ProducerRecord<>(topic, (String) null, "defg")).get();
        producer.flush();

        final var records = drainRecords(topic);

        assertThat(records).hasSize(1);
        assertThat(records.get(0).value()).isEqualTo("defg");
    }

    @Test
    void shouldHandleNullValueInByteEstimation() throws Exception {
        // ("abc",null) = 3+0=3 bytes; ("defg",null) = 4+0=4 bytes.
        // Budget 5: newest ("defg",null, 4B) accumulates 4<=5 → include; next ("abc",null) accumulates 7>5 → drop.
        final var topic = "retention-bytes-null-value-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "delete", "retention.bytes", "5"));

        producer.send(new ProducerRecord<>(topic, "abc", (String) null)).get();
        producer.send(new ProducerRecord<>(topic, "defg", (String) null)).get();
        producer.flush();

        final var records = drainRecords(topic);

        assertThat(records).hasSize(1);
        assertThat(records.get(0).key()).isEqualTo("defg");
    }

    // -------------------------------------------------------------------------
    // Combined policy tests
    // -------------------------------------------------------------------------

    @Test
    void shouldApplyRetentionMsAfterCompaction() throws Exception {
        // A compacted topic also configured with retention.ms: compaction runs first, then
        // time-based filtering removes any remaining expired records.
        final var topic = "compact-with-retention-ms-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "compact", "retention.ms", "5000"));

        final long expiredTimestamp = System.currentTimeMillis() - 10_000L;
        // k1 has only an expired record — after compaction it survives as the latest, but
        // retention.ms should then remove it because its timestamp is outside the window.
        producer.send(new ProducerRecord<>(topic, 0, expiredTimestamp, "k1", "old-value")).get();
        // k2 has a live record; it should pass both compaction and retention.ms filtering.
        producer.send(new ProducerRecord<>(topic, "k2", "live-value")).get();
        producer.flush();

        final var records = drainRecords(topic);

        assertThat(records.stream().filter(r -> "k1".equals(r.key())).collect(Collectors.toList())).isEmpty();
        final var k2Records = records.stream().filter(r -> "k2".equals(r.key())).collect(Collectors.toList());
        assertThat(k2Records).hasSize(1);
        assertThat(k2Records.get(0).value()).isEqualTo("live-value");
    }

    // -------------------------------------------------------------------------
    // compact,delete combined policy tests
    // -------------------------------------------------------------------------

    @Test
    void shouldApplyCompactionForCompactDeletePolicy() throws Exception {
        final var topic = "compact-delete-compaction-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "compact,delete"));

        producer.send(new ProducerRecord<>(topic, "k1", "v1")).get();
        producer.send(new ProducerRecord<>(topic, "k1", "v2")).get();
        producer.send(new ProducerRecord<>(topic, "k2", "va")).get();
        producer.flush();

        final var records = drainRecords(topic);

        final var k1Records = records.stream().filter(r -> "k1".equals(r.key())).collect(Collectors.toList());
        assertThat(k1Records).hasSize(1);
        assertThat(k1Records.get(0).value()).isEqualTo("v2");
        assertThat(records.stream().filter(r -> "k2".equals(r.key())).collect(Collectors.toList())).hasSize(1);
    }

    @Test
    void shouldApplyRetentionMsForCompactDeletePolicy() throws Exception {
        final var topic = "compact-delete-retention-ms-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "compact,delete", "retention.ms", "5000"));

        final long expiredTimestamp = System.currentTimeMillis() - 10_000L;
        producer.send(new ProducerRecord<>(topic, 0, expiredTimestamp, "k1", "old-value")).get();
        producer.send(new ProducerRecord<>(topic, "k2", "live-value")).get();
        producer.flush();

        final var records = drainRecords(topic);

        assertThat(records.stream().filter(r -> "k1".equals(r.key())).collect(Collectors.toList())).isEmpty();
        final var k2Records = records.stream().filter(r -> "k2".equals(r.key())).collect(Collectors.toList());
        assertThat(k2Records).hasSize(1);
        assertThat(k2Records.get(0).value()).isEqualTo("live-value");
    }

    @Test
    void shouldApplyCompactionAndRetentionMsForDeleteCompactPolicy() throws Exception {
        // Verify that "delete,compact" is treated identically to "compact,delete"
        final var topic = "delete-compact-" + UUID.randomUUID();
        createTopic(topic, Map.of("cleanup.policy", "delete,compact", "retention.ms", "5000"));

        final long expiredTimestamp = System.currentTimeMillis() - 10_000L;
        producer.send(new ProducerRecord<>(topic, 0, expiredTimestamp, "k1", "old-v1")).get();
        producer.send(new ProducerRecord<>(topic, 0, expiredTimestamp, "k1", "old-v2")).get();
        producer.send(new ProducerRecord<>(topic, "k2", "live-value")).get();
        producer.flush();

        final var records = drainRecords(topic);

        // k1 compacted to latest old-v2, then removed by retention.ms
        assertThat(records.stream().filter(r -> "k1".equals(r.key())).collect(Collectors.toList())).isEmpty();
        final var k2Records = records.stream().filter(r -> "k2".equals(r.key())).collect(Collectors.toList());
        assertThat(k2Records).hasSize(1);
        assertThat(k2Records.get(0).value()).isEqualTo("live-value");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void createTopic(final String name, final Map<String, String> configs) throws Exception {
        adminClient.createTopics(List.of(
            new NewTopic(name, 1, (short) 1).configs(configs)
        )).all().get();
    }

    private List<ConsumerRecord<String, String>> drainRecords(final String topic) throws Exception {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (final var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            // Obtain partition assignment
            for (int i = 0; i < 25 && consumer.assignment().isEmpty(); i++) {
                consumer.poll(Duration.ofMillis(200));
            }
            consumer.seekToBeginning(consumer.assignment());

            final var collected = new ArrayList<ConsumerRecord<String, String>>();
            var emptyPolls = 0;
            while (emptyPolls < 3) {
                final var batch = consumer.poll(Duration.ofMillis(300));
                if (batch.isEmpty()) {
                    emptyPolls++;
                } else {
                    emptyPolls = 0;
                    batch.forEach(collected::add);
                }
            }
            return copyOf(collected);
        }
    }
}
