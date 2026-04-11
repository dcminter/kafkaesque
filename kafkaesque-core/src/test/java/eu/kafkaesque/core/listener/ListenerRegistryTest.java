package eu.kafkaesque.core.listener;

import eu.kafkaesque.core.storage.StoredRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.List.of;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ListenerRegistry}.
 */
class ListenerRegistryTest {

    private ListenerRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ListenerRegistry();
    }

    @AfterEach
    void tearDown() {
        registry.close();
    }

    // --- RecordPublishedListener ---

    @Test
    void fireRecordPublished_shouldCallRegisteredListener() throws InterruptedException {
        final var latch = new CountDownLatch(1);
        final List<StoredRecord> received = new CopyOnWriteArrayList<>();
        registry.addRecordPublishedListener(r -> { received.add(r); latch.countDown(); });

        final var record = new StoredRecord("topic", 0, 0L, 1000L, of(), "key", "value");
        registry.fireRecordPublished(record);

        assertThat(latch.await(5, SECONDS)).isTrue();
        assertThat(received).containsExactly(record);
    }

    @Test
    void fireRecordPublished_shouldCallMultipleListeners() throws InterruptedException {
        final var latch = new CountDownLatch(2);
        final List<StoredRecord> received1 = new CopyOnWriteArrayList<>();
        final List<StoredRecord> received2 = new CopyOnWriteArrayList<>();
        registry.addRecordPublishedListener(r -> { received1.add(r); latch.countDown(); });
        registry.addRecordPublishedListener(r -> { received2.add(r); latch.countDown(); });

        final var record = new StoredRecord("topic", 0, 0L, 1000L, of(), "key", "value");
        registry.fireRecordPublished(record);

        assertThat(latch.await(5, SECONDS)).isTrue();
        assertThat(received1).containsExactly(record);
        assertThat(received2).containsExactly(record);
    }

    @Test
    void fireRecordPublished_shouldNotFailWithNoListeners() {
        final var record = new StoredRecord("topic", 0, 0L, 1000L, of(), "key", "value");
        registry.fireRecordPublished(record);
        // No exception means success
    }

    @Test
    void fireRecordPublished_shouldContinueAfterListenerException() throws InterruptedException {
        final var latch = new CountDownLatch(1);
        final var called = new AtomicBoolean(false);
        registry.addRecordPublishedListener(r -> { throw new RuntimeException("boom"); });
        registry.addRecordPublishedListener(r -> { called.set(true); latch.countDown(); });

        final var record = new StoredRecord("topic", 0, 0L, 1000L, of(), "key", "value");
        registry.fireRecordPublished(record);

        assertThat(latch.await(5, SECONDS)).isTrue();
        assertThat(called).isTrue();
    }

    // --- TopicCreatedListener ---

    @Test
    void fireTopicCreated_shouldCallRegisteredListener() throws InterruptedException {
        final var latch = new CountDownLatch(1);
        final List<String> received = new CopyOnWriteArrayList<>();
        registry.addTopicCreatedListener(t -> { received.add(t); latch.countDown(); });

        registry.fireTopicCreated("my-topic");

        assertThat(latch.await(5, SECONDS)).isTrue();
        assertThat(received).containsExactly("my-topic");
    }

    @Test
    void fireTopicCreated_shouldCallMultipleListeners() throws InterruptedException {
        final var latch = new CountDownLatch(2);
        final List<String> received1 = new CopyOnWriteArrayList<>();
        final List<String> received2 = new CopyOnWriteArrayList<>();
        registry.addTopicCreatedListener(t -> { received1.add(t); latch.countDown(); });
        registry.addTopicCreatedListener(t -> { received2.add(t); latch.countDown(); });

        registry.fireTopicCreated("my-topic");

        assertThat(latch.await(5, SECONDS)).isTrue();
        assertThat(received1).containsExactly("my-topic");
        assertThat(received2).containsExactly("my-topic");
    }

    @Test
    void fireTopicCreated_shouldNotFailWithNoListeners() {
        registry.fireTopicCreated("my-topic");
    }

    @Test
    void fireTopicCreated_shouldContinueAfterListenerException() throws InterruptedException {
        final var latch = new CountDownLatch(1);
        final var called = new AtomicBoolean(false);
        registry.addTopicCreatedListener(t -> { throw new RuntimeException("boom"); });
        registry.addTopicCreatedListener(t -> { called.set(true); latch.countDown(); });

        registry.fireTopicCreated("my-topic");

        assertThat(latch.await(5, SECONDS)).isTrue();
        assertThat(called).isTrue();
    }

    // --- TransactionCompletedListener ---

    @Test
    void fireTransactionCompleted_shouldCallRegisteredListenerOnCommit() throws InterruptedException {
        final var latch = new CountDownLatch(1);
        final List<String> receivedIds = new CopyOnWriteArrayList<>();
        final List<Boolean> receivedCommitted = new CopyOnWriteArrayList<>();
        registry.addTransactionCompletedListener((final String id, final boolean committed) -> {
            receivedIds.add(id);
            receivedCommitted.add(committed);
            latch.countDown();
        });

        registry.fireTransactionCompleted("txn-1", true);

        assertThat(latch.await(5, SECONDS)).isTrue();
        assertThat(receivedIds).containsExactly("txn-1");
        assertThat(receivedCommitted).containsExactly(true);
    }

    @Test
    void fireTransactionCompleted_shouldCallRegisteredListenerOnAbort() throws InterruptedException {
        final var latch = new CountDownLatch(1);
        final List<String> receivedIds = new CopyOnWriteArrayList<>();
        final List<Boolean> receivedCommitted = new CopyOnWriteArrayList<>();
        registry.addTransactionCompletedListener((final String id, final boolean committed) -> {
            receivedIds.add(id);
            receivedCommitted.add(committed);
            latch.countDown();
        });

        registry.fireTransactionCompleted("txn-1", false);

        assertThat(latch.await(5, SECONDS)).isTrue();
        assertThat(receivedIds).containsExactly("txn-1");
        assertThat(receivedCommitted).containsExactly(false);
    }

    @Test
    void fireTransactionCompleted_shouldCallMultipleListeners() throws InterruptedException {
        final var latch = new CountDownLatch(2);
        final List<String> received1 = new CopyOnWriteArrayList<>();
        final List<String> received2 = new CopyOnWriteArrayList<>();
        registry.addTransactionCompletedListener((final String id, final boolean c) -> {
            received1.add(id); latch.countDown();
        });
        registry.addTransactionCompletedListener((final String id, final boolean c) -> {
            received2.add(id); latch.countDown();
        });

        registry.fireTransactionCompleted("txn-1", true);

        assertThat(latch.await(5, SECONDS)).isTrue();
        assertThat(received1).containsExactly("txn-1");
        assertThat(received2).containsExactly("txn-1");
    }

    @Test
    void fireTransactionCompleted_shouldNotFailWithNoListeners() {
        registry.fireTransactionCompleted("txn-1", true);
    }

    @Test
    void fireTransactionCompleted_shouldContinueAfterListenerException() throws InterruptedException {
        final var latch = new CountDownLatch(1);
        final var called = new AtomicBoolean(false);
        registry.addTransactionCompletedListener(
            (final String id, final boolean c) -> { throw new RuntimeException("boom"); });
        registry.addTransactionCompletedListener(
            (final String id, final boolean c) -> { called.set(true); latch.countDown(); });

        registry.fireTransactionCompleted("txn-1", true);

        assertThat(latch.await(5, SECONDS)).isTrue();
        assertThat(called).isTrue();
    }
}
