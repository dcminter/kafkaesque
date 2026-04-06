package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.List.of;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Abstract base class defining the transaction integration test suite.
 *
 * <p>Covers the following scenarios:</p>
 * <ul>
 *   <li>Committed transactions are visible to {@code READ_COMMITTED} consumers</li>
 *   <li>Aborted transactions are hidden from {@code READ_COMMITTED} consumers</li>
 *   <li>In-flight transactions are hidden from {@code READ_COMMITTED} consumers until commit</li>
 *   <li>In-flight transactions are visible to {@code READ_UNCOMMITTED} consumers</li>
 *   <li>Multiple sequential transactions on the same producer all commit correctly</li>
 *   <li>Non-transactional records produced after an in-flight transaction are hidden from
 *       {@code READ_COMMITTED} consumers until that transaction commits (LSO boundary)</li>
 * </ul>
 *
 * <p>Each test uses a sentinel (non-transactional) record produced before the transactional
 * records. This allows {@code READ_COMMITTED} consumers to confirm they have read up to the
 * point of the sentinel without seeing the (still-pending) transactional records beyond it.</p>
 *
 * <p>Subclasses must implement {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractTransactionBehaviorIT {

    /** Per-test topic name; unique per invocation to avoid cross-test interference. */
    private String topicName;

    /** Per-test transactional ID; unique per invocation. */
    private String transactionalId;

    /** Per-test transactional producer; closed in {@link #tearDownClients()}. */
    private KafkaProducer<String, String> txProducer;

    /** Per-test regular (non-transactional) producer; closed in {@link #tearDownClients()}. */
    private KafkaProducer<String, String> regularProducer;

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Creates fresh producers for each test.
     *
     * @throws Exception if the clients cannot be created
     */
    @BeforeEach
    void setUpClients() throws Exception {
        final String bootstrapServers = getBootstrapServers();
        topicName = "test-txn-topic-" + UUID.randomUUID();
        transactionalId = "test-txn-id-" + UUID.randomUUID();
        txProducer = KafkaTestClientFactory.createTransactionalProducer(bootstrapServers, transactionalId);
        regularProducer = KafkaTestClientFactory.createProducer(bootstrapServers);
        txProducer.initTransactions();
    }

    /**
     * Closes all producers and consumers after each test.
     */
    @AfterEach
    void tearDownClients() {
        if (txProducer != null) {
            txProducer.close();
        }
        if (regularProducer != null) {
            regularProducer.close();
        }
    }

    /**
     * Verifies that records produced within a committed transaction are visible to a
     * {@code READ_COMMITTED} consumer.
     */
    @Test
    void shouldMakeCommittedTransactionalRecordsVisibleToReadCommittedConsumer() throws Exception {
        // Given – produce two records in a transaction and commit
        txProducer.beginTransaction();
        txProducer.send(new ProducerRecord<>(topicName, "key-1", "value-1")).get();
        txProducer.send(new ProducerRecord<>(topicName, "key-2", "value-2")).get();
        txProducer.commitTransaction();

        log.info("Committed transaction for topic {}", topicName);

        // When – READ_COMMITTED consumer subscribes and polls
        try (var consumer = KafkaTestClientFactory.createReadCommittedConsumer(getBootstrapServers())) {
            consumer.subscribe(of(topicName));

            final List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(received::add);
                return received.size() >= 2;
            });

            // Then – both committed records are visible
            assertThat(received).hasSize(2);
            assertThat(received).extracting(ConsumerRecord::value)
                .containsExactly("value-1", "value-2");
        }

        log.info("shouldMakeCommittedTransactionalRecordsVisibleToReadCommittedConsumer passed");
    }

    /**
     * Verifies that records produced within an aborted transaction are hidden from a
     * {@code READ_COMMITTED} consumer.
     *
     * <p>A non-transactional sentinel record is produced before the aborted transaction to give the
     * consumer a stable read point. After the abort, the consumer should receive only the sentinel
     * and none of the aborted records.</p>
     */
    @Test
    void shouldHideAbortedTransactionalRecordsFromReadCommittedConsumer() throws Exception {
        // Given – produce a sentinel, then a transaction that will be aborted
        regularProducer.send(new ProducerRecord<>(topicName, "sentinel", "sentinel-value")).get();
        regularProducer.flush();

        txProducer.beginTransaction();
        txProducer.send(new ProducerRecord<>(topicName, "aborted-1", "aborted-value-1")).get();
        txProducer.send(new ProducerRecord<>(topicName, "aborted-2", "aborted-value-2")).get();
        txProducer.abortTransaction();

        log.info("Aborted transaction for topic {}", topicName);

        // When – READ_COMMITTED consumer subscribes and polls until it sees the sentinel
        try (var consumer = KafkaTestClientFactory.createReadCommittedConsumer(getBootstrapServers())) {
            consumer.subscribe(of(topicName));

            final List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(received::add);
                return received.stream().anyMatch(r -> "sentinel".equals(r.key()));
            });

            // Then – only the sentinel is visible; aborted records are hidden
            assertThat(received).hasSize(1);
            assertThat(received.get(0).key()).isEqualTo("sentinel");
        }

        log.info("shouldHideAbortedTransactionalRecordsFromReadCommittedConsumer passed");
    }

    /**
     * Verifies that in-flight (not yet committed) transactional records are hidden from a
     * {@code READ_COMMITTED} consumer, and become visible once the transaction commits.
     *
     * <p>A sentinel record is produced before the transaction. The {@code READ_COMMITTED} consumer
     * should receive the sentinel but not the in-flight records. After committing, the consumer
     * should receive the transactional records on the next poll.</p>
     */
    @Test
    void shouldHideInFlightRecordsFromReadCommittedConsumerThenRevealAfterCommit() throws Exception {
        // Given – produce sentinel before the transaction
        regularProducer.send(new ProducerRecord<>(topicName, "sentinel", "sentinel-value")).get();
        regularProducer.flush();

        // Begin transaction and flush records without committing
        txProducer.beginTransaction();
        txProducer.send(new ProducerRecord<>(topicName, "tx-key-1", "tx-value-1")).get();
        txProducer.send(new ProducerRecord<>(topicName, "tx-key-2", "tx-value-2")).get();
        txProducer.flush();

        log.info("Produced transactional records (not committed) for topic {}", topicName);

        try (var consumer = KafkaTestClientFactory.createReadCommittedConsumer(getBootstrapServers())) {
            consumer.subscribe(of(topicName));

            // When – poll until sentinel is received (proves consumer reached the sentinel offset)
            final List<ConsumerRecord<String, String>> phase1 = new ArrayList<>();
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(phase1::add);
                return phase1.stream().anyMatch(r -> "sentinel".equals(r.key()));
            });

            // Then – only the sentinel is visible; in-flight records are hidden
            assertThat(phase1).hasSize(1);
            assertThat(phase1.get(0).key()).isEqualTo("sentinel");

            // When – commit the transaction
            txProducer.commitTransaction();
            log.info("Committed transaction for topic {}", topicName);

            // Then – the transactional records are now visible
            final List<ConsumerRecord<String, String>> phase2 = new ArrayList<>();
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(phase2::add);
                return phase2.size() >= 2;
            });

            assertThat(phase2).hasSize(2);
            assertThat(phase2).extracting(ConsumerRecord::key)
                .containsExactlyInAnyOrder("tx-key-1", "tx-key-2");
        }

        log.info("shouldHideInFlightRecordsFromReadCommittedConsumerThenRevealAfterCommit passed");
    }

    /**
     * Verifies that in-flight (not yet committed) transactional records are visible to a
     * {@code READ_UNCOMMITTED} consumer.
     *
     * <p>The {@code READ_UNCOMMITTED} consumer bypasses the last-stable-offset bound, so it
     * receives all records including those from open transactions.</p>
     */
    @Test
    void shouldShowInFlightTransactionalRecordsToReadUncommittedConsumer() throws Exception {
        // Given – produce sentinel then begin a transaction without committing
        regularProducer.send(new ProducerRecord<>(topicName, "sentinel", "sentinel-value")).get();
        regularProducer.flush();

        txProducer.beginTransaction();
        txProducer.send(new ProducerRecord<>(topicName, "tx-key", "tx-value")).get();
        txProducer.flush();

        log.info("Produced transactional record (not committed) for topic {}", topicName);

        // When – READ_UNCOMMITTED consumer polls
        try (var consumer = KafkaTestClientFactory.createReadUncommittedConsumer(getBootstrapServers())) {
            consumer.subscribe(of(topicName));

            final List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(received::add);
                return received.size() >= 2;
            });

            // Then – both sentinel and in-flight transactional record are visible
            assertThat(received).hasSize(2);
            assertThat(received).extracting(ConsumerRecord::key)
                .containsExactlyInAnyOrder("sentinel", "tx-key");
        }

        // Clean up – abort the open transaction
        txProducer.abortTransaction();

        log.info("shouldShowInFlightTransactionalRecordsToReadUncommittedConsumer passed");
    }

    /**
     * Verifies that multiple sequential transactions on the same producer all commit correctly
     * and that all produced records are visible to a {@code READ_COMMITTED} consumer.
     */
    @Test
    void shouldHandleMultipleSequentialTransactionsOnSameProducer() throws Exception {
        // Given – two sequential transactions
        txProducer.beginTransaction();
        txProducer.send(new ProducerRecord<>(topicName, "key-tx1", "value-tx1")).get();
        txProducer.commitTransaction();

        txProducer.beginTransaction();
        txProducer.send(new ProducerRecord<>(topicName, "key-tx2", "value-tx2")).get();
        txProducer.commitTransaction();

        log.info("Committed two transactions for topic {}", topicName);

        // When – READ_COMMITTED consumer polls
        try (var consumer = KafkaTestClientFactory.createReadCommittedConsumer(getBootstrapServers())) {
            consumer.subscribe(of(topicName));

            final List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(received::add);
                return received.size() >= 2;
            });

            // Then – records from both transactions are visible
            assertThat(received).hasSize(2);
            assertThat(received).extracting(ConsumerRecord::key)
                .containsExactly("key-tx1", "key-tx2");
        }

        log.info("shouldHandleMultipleSequentialTransactionsOnSameProducer passed");
    }

    /**
     * Verifies that records from an aborted transaction are visible to a
     * {@code READ_UNCOMMITTED} consumer.
     *
     * <p>Real Kafka's contract states: "consumer.poll() will return all messages,
     * even transactional messages which have been aborted." Unlike
     * {@code READ_COMMITTED} consumers, which see only committed records,
     * {@code READ_UNCOMMITTED} consumers bypass transaction filtering entirely and
     * see every record in the partition log regardless of its transaction outcome.</p>
     *
     * <p>A non-transactional sentinel is produced before the aborted transaction so
     * the consumer has a reference point. All three records (sentinel and both
     * aborted records) must be present in the consumer's results.</p>
     */
    @Test
    void shouldShowAbortedTransactionalRecordsToReadUncommittedConsumer() throws Exception {
        // Given – produce a sentinel, then a transaction that will be aborted
        regularProducer.send(new ProducerRecord<>(topicName, "sentinel", "sentinel-value")).get();
        regularProducer.flush();

        txProducer.beginTransaction();
        txProducer.send(new ProducerRecord<>(topicName, "aborted-key-1", "aborted-value-1")).get();
        txProducer.send(new ProducerRecord<>(topicName, "aborted-key-2", "aborted-value-2")).get();
        txProducer.abortTransaction();

        log.info("Aborted transaction for topic {}", topicName);

        // When – READ_UNCOMMITTED consumer polls until all 3 records are received
        try (var consumer = KafkaTestClientFactory.createReadUncommittedConsumer(getBootstrapServers())) {
            consumer.subscribe(of(topicName));

            final List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(received::add);
                return received.size() >= 3;
            });

            // Then – all three records are visible, including the aborted ones
            assertThat(received).hasSize(3);
            assertThat(received).extracting(ConsumerRecord::key)
                .containsExactlyInAnyOrder("sentinel", "aborted-key-1", "aborted-key-2");
        }

        log.info("shouldShowAbortedTransactionalRecordsToReadUncommittedConsumer passed");
    }

    /**
     * Verifies that non-transactional records and committed transactional records on the same
     * topic are both visible to a {@code READ_COMMITTED} consumer, while aborted transactional
     * records on the same topic remain hidden.
     */
    @Test
    void shouldMixTransactionalAndNonTransactionalRecordsCorrectly() throws Exception {
        // Given – non-transactional "A", committed transaction "B", aborted transaction "C"
        regularProducer.send(new ProducerRecord<>(topicName, "key-A", "value-A")).get();
        regularProducer.flush();

        txProducer.beginTransaction();
        txProducer.send(new ProducerRecord<>(topicName, "key-B", "value-B")).get();
        txProducer.commitTransaction();

        txProducer.beginTransaction();
        txProducer.send(new ProducerRecord<>(topicName, "key-C", "value-C")).get();
        txProducer.abortTransaction();

        // Produce a sentinel to confirm the consumer has processed past the abort
        regularProducer.send(new ProducerRecord<>(topicName, "sentinel", "sentinel-value")).get();
        regularProducer.flush();

        log.info("Produced mixed records for topic {}", topicName);

        // When – READ_COMMITTED consumer polls until it sees the sentinel
        try (var consumer = KafkaTestClientFactory.createReadCommittedConsumer(getBootstrapServers())) {
            consumer.subscribe(of(topicName));

            final List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(received::add);
                return received.stream().anyMatch(r -> "sentinel".equals(r.key()));
            });

            // Then – A, B, and sentinel are visible; C (aborted) is hidden
            assertThat(received).extracting(ConsumerRecord::key)
                .containsExactly("key-A", "key-B", "sentinel");
        }

        log.info("shouldMixTransactionalAndNonTransactionalRecordsCorrectly passed");
    }

    /**
     * Verifies that the LSO boundary hides non-transactional records produced after an
     * in-flight transaction from a {@code READ_COMMITTED} consumer until that transaction commits.
     *
     * <p>When a transactional record at offset 0 is pending, the LSO is 0. Any record
     * produced after it — even a non-transactional record at offset 1 — falls at or above
     * the LSO and is therefore hidden from {@code READ_COMMITTED} consumers until the open
     * transaction commits.</p>
     *
     * <p>Scenario:</p>
     * <ol>
     *   <li>Begin transaction; produce "tx-key" (offset 0) without committing.</li>
     *   <li>Produce non-transactional "after-key" (offset 1).</li>
     *   <li>{@code READ_COMMITTED} consumer sees nothing (LSO = 0 blocks all records).</li>
     *   <li>Commit the transaction.</li>
     *   <li>{@code READ_COMMITTED} consumer now sees both records in offset order.</li>
     * </ol>
     */
    @Test
    void shouldHideNonTransactionalRecordProducedAfterInFlightTransactionFromReadCommittedConsumer()
            throws Exception {
        // Given – begin a transaction and produce a record without committing
        txProducer.beginTransaction();
        txProducer.send(new ProducerRecord<>(topicName, "tx-key", "tx-value")).get();
        txProducer.flush();

        // Produce a non-transactional record after the open transaction
        regularProducer.send(new ProducerRecord<>(topicName, "after-key", "after-value")).get();
        regularProducer.flush();

        log.info("Produced tx record (uncommitted) and after record for topic {}", topicName);

        try (var consumer = KafkaTestClientFactory.createReadCommittedConsumer(getBootstrapServers())) {
            consumer.subscribe(of(topicName));

            // When (phase 1) – poll for 2 seconds; READ_COMMITTED must see nothing
            // because LSO=0 (the pending tx at offset 0 blocks everything at or above it)
            final List<ConsumerRecord<String, String>> phase1 = new ArrayList<>();
            final long deadline = System.currentTimeMillis() + 2000;
            while (System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(phase1::add);
            }

            // Then – nothing visible while the transaction is open
            assertThat(phase1).isEmpty();

            // When (phase 2) – commit the transaction
            txProducer.commitTransaction();
            log.info("Committed transaction for topic {}", topicName);

            // Then – both records are now visible in offset order
            final List<ConsumerRecord<String, String>> phase2 = new ArrayList<>();
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(phase2::add);
                return phase2.size() >= 2;
            });

            assertThat(phase2).hasSize(2);
            assertThat(phase2).extracting(ConsumerRecord::key)
                .containsExactly("tx-key", "after-key");
        }

        log.info("shouldHideNonTransactionalRecordProducedAfterInFlightTransactionFromReadCommittedConsumer passed");
    }
}
