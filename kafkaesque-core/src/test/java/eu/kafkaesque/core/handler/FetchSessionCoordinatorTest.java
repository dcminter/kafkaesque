package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.handler.FetchSessionCoordinator.FetchSession;
import eu.kafkaesque.core.handler.FetchSessionCoordinator.PartitionFetchState;
import eu.kafkaesque.core.handler.FetchSessionCoordinator.SessionResult;
import eu.kafkaesque.core.handler.FetchSessionCoordinator.TopicPartitionKey;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Unit tests for {@link FetchSessionCoordinator}.
 */
class FetchSessionCoordinatorTest {

    private FetchSessionCoordinator coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new FetchSessionCoordinator();
    }

    @Test
    void createSession_shouldAssignUniqueSessionId() {
        final var partitions = Map.of(
            new TopicPartitionKey("my-topic", 0), new PartitionFetchState(0L, 1024));

        final FetchSession session = coordinator.createSession(partitions);

        assertThat(session.sessionId()).isPositive();
        assertThat(session.nextEpoch()).isEqualTo(1);
        assertThat(session.partitions()).containsKey(new TopicPartitionKey("my-topic", 0));
    }

    @Test
    void createSession_multipleSessions_shouldHaveDistinctIds() {
        final var partitions = Map.of(
            new TopicPartitionKey("topic", 0), new PartitionFetchState(0L, 1024));

        final int id1 = coordinator.createSession(partitions).sessionId();
        final int id2 = coordinator.createSession(partitions).sessionId();

        assertThat(id1).isNotEqualTo(id2);
    }

    @Test
    void updateSession_validEpoch_shouldMergePartitionsAndAdvanceEpoch() {
        final var initialPartitions = Map.of(
            new TopicPartitionKey("topic", 0), new PartitionFetchState(0L, 1024));
        final var session = coordinator.createSession(initialPartitions);

        final var updates = Map.of(
            new TopicPartitionKey("topic", 1), new PartitionFetchState(5L, 2048));
        final SessionResult result = coordinator.updateSession(
            session.sessionId(), 1, updates, List.of());

        assertThat(result.isError()).isFalse();
        assertThat(result.session().partitions()).containsKey(new TopicPartitionKey("topic", 0));
        assertThat(result.session().partitions()).containsKey(new TopicPartitionKey("topic", 1));
        assertThat(result.session().nextEpoch()).isEqualTo(2);
    }

    @Test
    void updateSession_updatedFetchOffset_shouldOverwriteExistingEntry() {
        final var initialPartitions = Map.of(
            new TopicPartitionKey("topic", 0), new PartitionFetchState(0L, 1024));
        final var session = coordinator.createSession(initialPartitions);

        final var updates = Map.of(
            new TopicPartitionKey("topic", 0), new PartitionFetchState(10L, 1024));
        final SessionResult result = coordinator.updateSession(
            session.sessionId(), 1, updates, List.of());

        assertThat(result.isError()).isFalse();
        assertThat(result.session().partitions().get(new TopicPartitionKey("topic", 0)).fetchOffset())
            .isEqualTo(10L);
    }

    @Test
    void updateSession_withForgottenTopic_shouldRemovePartition() {
        final var initialPartitions = Map.of(
            new TopicPartitionKey("topic", 0), new PartitionFetchState(0L, 1024),
            new TopicPartitionKey("topic", 1), new PartitionFetchState(0L, 1024));
        final var session = coordinator.createSession(initialPartitions);

        final var forgotten = new FetchRequestData.ForgottenTopic()
            .setTopic("topic")
            .setPartitions(List.of(1));
        final SessionResult result = coordinator.updateSession(
            session.sessionId(), 1, Map.of(), List.of(forgotten));

        assertThat(result.isError()).isFalse();
        assertThat(result.session().partitions()).containsKey(new TopicPartitionKey("topic", 0));
        assertThat(result.session().partitions()).doesNotContainKey(new TopicPartitionKey("topic", 1));
    }

    @Test
    void updateSession_unknownSessionId_shouldReturnNotFoundError() {
        final SessionResult result = coordinator.updateSession(
            99999, 1, Map.of(), List.of());

        assertThat(result.isError()).isTrue();
        assertThat(result.errorCode()).isEqualTo(Errors.FETCH_SESSION_ID_NOT_FOUND.code());
    }

    @Test
    void updateSession_wrongEpoch_shouldReturnInvalidEpochError() {
        final var session = coordinator.createSession(Map.of(
            new TopicPartitionKey("topic", 0), new PartitionFetchState(0L, 1024)));

        final SessionResult result = coordinator.updateSession(
            session.sessionId(), 5, Map.of(), List.of());

        assertThat(result.isError()).isTrue();
        assertThat(result.errorCode()).isEqualTo(Errors.INVALID_FETCH_SESSION_EPOCH.code());
    }

    @Test
    void closeSession_shouldMakeSessionUnavailable() {
        final var session = coordinator.createSession(Map.of(
            new TopicPartitionKey("topic", 0), new PartitionFetchState(0L, 1024)));

        coordinator.closeSession(session.sessionId());

        final SessionResult result = coordinator.updateSession(
            session.sessionId(), 1, Map.of(), List.of());
        assertThat(result.isError()).isTrue();
        assertThat(result.errorCode()).isEqualTo(Errors.FETCH_SESSION_ID_NOT_FOUND.code());
    }

    @Test
    void closeSession_nonExistentId_shouldBeNoOp() {
        assertThatNoException().isThrownBy(() -> coordinator.closeSession(99999));
    }

    @Test
    void updateSession_multipleSequentialUpdates_shouldAdvanceEpochCorrectly() {
        final var session = coordinator.createSession(Map.of(
            new TopicPartitionKey("topic", 0), new PartitionFetchState(0L, 1024)));

        final SessionResult first = coordinator.updateSession(session.sessionId(), 1, Map.of(), List.of());
        assertThat(first.isError()).isFalse();
        assertThat(first.session().nextEpoch()).isEqualTo(2);

        final SessionResult second = coordinator.updateSession(session.sessionId(), 2, Map.of(), List.of());
        assertThat(second.isError()).isFalse();
        assertThat(second.session().nextEpoch()).isEqualTo(3);
    }

    @Test
    void createSession_inputMapMutationAfterCreation_shouldNotAffectSession() {
        final var mutablePartitions = new HashMap<TopicPartitionKey, PartitionFetchState>();
        mutablePartitions.put(new TopicPartitionKey("topic", 0), new PartitionFetchState(0L, 1024));
        final var session = coordinator.createSession(mutablePartitions);

        mutablePartitions.put(new TopicPartitionKey("other-topic", 0), new PartitionFetchState(0L, 512));

        assertThat(session.partitions()).doesNotContainKey(new TopicPartitionKey("other-topic", 0));
    }

    @Test
    void clear_shouldRemoveAllSessions() {
        coordinator.createSession(Map.of(
            new TopicPartitionKey("topic", 0), new PartitionFetchState(0L, 1024)));
        coordinator.createSession(Map.of(
            new TopicPartitionKey("topic", 1), new PartitionFetchState(0L, 1024)));

        coordinator.clear();

        // New sessions after clear should start from a fresh ID range but the key test
        // is that previously-stored sessions are no longer accessible
        final SessionResult result = coordinator.updateSession(1, 1, Map.of(), List.of());
        assertThat(result.isError()).isTrue();
    }
}
