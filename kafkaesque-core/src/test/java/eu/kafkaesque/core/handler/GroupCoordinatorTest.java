package eu.kafkaesque.core.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link GroupCoordinator}.
 */
class GroupCoordinatorTest {

    private GroupCoordinator coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new GroupCoordinator();
    }

    @Test
    void joinGroup_withBlankMemberId_shouldGenerateUniqueMemberId() {
        final var memberId = coordinator.joinGroup("group-1", "");

        assertThat(memberId).isNotBlank().startsWith("group-1-");
    }

    @Test
    void joinGroup_withNullMemberId_shouldGenerateUniqueMemberId() {
        final var memberId = coordinator.joinGroup("group-1", null);

        assertThat(memberId).isNotBlank().startsWith("group-1-");
    }

    @Test
    void joinGroup_withExistingMemberId_shouldReuseSuppliedId() {
        final var memberId = coordinator.joinGroup("group-1", "existing-member-id");

        assertThat(memberId).isEqualTo("existing-member-id");
    }

    @Test
    void joinGroup_differentGroups_shouldBeIndependent() {
        final var memberId1 = coordinator.joinGroup("group-1", "");
        final var memberId2 = coordinator.joinGroup("group-2", "");

        assertThat(memberId1).isNotEqualTo(memberId2);
    }

    @Test
    void getGenerationId_forKnownGroup_shouldReturnOne() {
        coordinator.joinGroup("group-1", "member-1");

        assertThat(coordinator.getGenerationId("group-1")).isEqualTo(1);
    }

    @Test
    void getGenerationId_forUnknownGroup_shouldReturnOne() {
        assertThat(coordinator.getGenerationId("unknown-group")).isEqualTo(1);
    }

    @Test
    void syncGroup_shouldStoreAssignments() {
        coordinator.joinGroup("group-1", "member-1");
        final var assignmentBytes = new byte[]{1, 2, 3};
        coordinator.syncGroup("group-1", Map.of("member-1", assignmentBytes));

        assertThat(coordinator.getMemberAssignment("group-1", "member-1"))
            .isEqualTo(assignmentBytes);
    }

    @Test
    void syncGroup_forUnknownGroup_shouldNotFail() {
        // Should be a no-op rather than throwing
        coordinator.syncGroup("unknown-group", Map.of("member-1", new byte[]{1}));
        // No assertion needed — just verifying no exception is thrown
    }

    @Test
    void getMemberAssignment_withNoSyncGroupCalled_shouldReturnEmptyArray() {
        coordinator.joinGroup("group-1", "member-1");

        assertThat(coordinator.getMemberAssignment("group-1", "member-1")).isEmpty();
    }

    @Test
    void getMemberAssignment_forUnknownGroup_shouldReturnEmptyArray() {
        assertThat(coordinator.getMemberAssignment("unknown-group", "member-1")).isEmpty();
    }

    @Test
    void commitOffset_shouldStoreOffsetForGroupAndPartition() {
        coordinator.commitOffset("group-1", "topic-a", 0, 42L);

        assertThat(coordinator.getCommittedOffset("group-1", "topic-a", 0)).isEqualTo(42L);
    }

    @Test
    void commitOffset_shouldBeIndependentAcrossPartitions() {
        coordinator.commitOffset("group-1", "topic-a", 0, 10L);
        coordinator.commitOffset("group-1", "topic-a", 1, 20L);

        assertThat(coordinator.getCommittedOffset("group-1", "topic-a", 0)).isEqualTo(10L);
        assertThat(coordinator.getCommittedOffset("group-1", "topic-a", 1)).isEqualTo(20L);
    }

    @Test
    void commitOffset_shouldBeIndependentAcrossGroups() {
        coordinator.commitOffset("group-1", "topic-a", 0, 5L);
        coordinator.commitOffset("group-2", "topic-a", 0, 99L);

        assertThat(coordinator.getCommittedOffset("group-1", "topic-a", 0)).isEqualTo(5L);
        assertThat(coordinator.getCommittedOffset("group-2", "topic-a", 0)).isEqualTo(99L);
    }

    @Test
    void getCommittedOffset_withNoCommit_shouldReturnMinusOne() {
        assertThat(coordinator.getCommittedOffset("group-1", "topic-a", 0)).isEqualTo(-1L);
    }

    @Test
    void clear_shouldRemoveAllGroupStateAndOffsets() {
        coordinator.joinGroup("group-1", "member-1");
        coordinator.commitOffset("group-1", "topic-a", 0, 50L);

        coordinator.clear();

        assertThat(coordinator.getCommittedOffset("group-1", "topic-a", 0)).isEqualTo(-1L);
        assertThat(coordinator.getMemberAssignment("group-1", "member-1")).isEmpty();
    }
}
