package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.EventStore;
import eu.kafkaesque.core.storage.RecordData;
import eu.kafkaesque.core.storage.TransactionState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TransactionCoordinator}.
 */
class TransactionCoordinatorTest {

    private EventStore eventStore;
    private TransactionCoordinator coordinator;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
        coordinator = new TransactionCoordinator(eventStore);
    }

    @Test
    void initProducerId_forNewTransactionalId_shouldAssignEpochZero() {
        final var result = coordinator.initProducerId("my-txn-id");

        assertThat(result.producerId()).isPositive();
        assertThat(result.epoch()).isZero();
    }

    @Test
    void initProducerId_forSameTransactionalId_shouldIncrementEpoch() {
        final var first = coordinator.initProducerId("my-txn-id");
        final var second = coordinator.initProducerId("my-txn-id");

        assertThat(second.producerId()).isEqualTo(first.producerId());
        assertThat(second.epoch()).isEqualTo((short) (first.epoch() + 1));
    }

    @Test
    void initProducerId_forNullId_shouldReturnEphemeralProducer() {
        final var result = coordinator.initProducerId(null);

        assertThat(result.producerId()).isPositive();
        assertThat(result.epoch()).isZero();
    }

    @Test
    void initProducerId_forBlankId_shouldReturnEphemeralProducer() {
        final var result = coordinator.initProducerId("  ");

        assertThat(result.producerId()).isPositive();
        assertThat(result.epoch()).isZero();
    }

    @Test
    void initProducerId_differentTransactionalIds_shouldHaveDifferentProducerIds() {
        final var first = coordinator.initProducerId("txn-1");
        final var second = coordinator.initProducerId("txn-2");

        assertThat(first.producerId()).isNotEqualTo(second.producerId());
    }

    @Test
    void addPendingOffsetCommit_shouldBufferCommitForLaterDrain() {
        coordinator.addPendingOffsetCommit("txn-1", "group-1", "topic-a", 0, 42L);

        final var commits = coordinator.drainPendingOffsetCommits("txn-1");

        assertThat(commits).hasSize(1);
        assertThat(commits.get(0).groupId()).isEqualTo("group-1");
        assertThat(commits.get(0).topic()).isEqualTo("topic-a");
        assertThat(commits.get(0).partitionIndex()).isZero();
        assertThat(commits.get(0).offset()).isEqualTo(42L);
    }

    @Test
    void drainPendingOffsetCommits_shouldClearBufferedCommits() {
        coordinator.addPendingOffsetCommit("txn-1", "group-1", "topic-a", 0, 42L);

        coordinator.drainPendingOffsetCommits("txn-1");
        final var secondDrain = coordinator.drainPendingOffsetCommits("txn-1");

        assertThat(secondDrain).isEmpty();
    }

    @Test
    void drainPendingOffsetCommits_forUnknownTransaction_shouldReturnEmptyList() {
        assertThat(coordinator.drainPendingOffsetCommits("unknown-txn")).isEmpty();
    }

    @Test
    void endTransaction_commit_shouldMakePendingRecordsVisible() {
        final var topic = "test-topic";
        final var timestamp = System.currentTimeMillis();
        eventStore.storePendingRecord("txn-1", new RecordData(topic, 0, timestamp, "k", "v", emptyList()));

        coordinator.endTransaction("txn-1", true);

        final var committed = eventStore.getRecords(topic, 0, (byte) 1);
        assertThat(committed).hasSize(1);
        assertThat(eventStore.getTransactionState(topic, 0, 0L)).isEqualTo(TransactionState.COMMITTED);
    }

    @Test
    void endTransaction_abort_shouldHidePendingRecords() {
        final var topic = "test-topic";
        final var timestamp = System.currentTimeMillis();
        eventStore.storePendingRecord("txn-1", new RecordData(topic, 0, timestamp, "k", "v", emptyList()));

        coordinator.endTransaction("txn-1", false);

        final var committed = eventStore.getRecords(topic, 0, (byte) 1);
        assertThat(committed).isEmpty();
        assertThat(eventStore.getTransactionState(topic, 0, 0L)).isEqualTo(TransactionState.ABORTED);
    }

    @Test
    void clear_shouldRemoveAllProducerState() {
        coordinator.initProducerId("txn-1");
        coordinator.addPendingOffsetCommit("txn-1", "group-1", "topic-a", 0, 1L);

        coordinator.clear();

        // After clear, a new init should assign epoch 0 again (as if it's a fresh producer)
        final var fresh = coordinator.initProducerId("txn-1");
        assertThat(fresh.epoch()).isZero();
        assertThat(coordinator.drainPendingOffsetCommits("txn-1")).isEmpty();
    }
}
