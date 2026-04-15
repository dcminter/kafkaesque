package eu.kafkaesque.core.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.util.Map.of;
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
        final var memberId = coordinator.joinGroup("group-1", "", new byte[0]);

        assertThat(memberId).isNotBlank().startsWith("group-1-");
    }

    @Test
    void joinGroup_withNullMemberId_shouldGenerateUniqueMemberId() {
        final var memberId = coordinator.joinGroup("group-1", null, new byte[0]);

        assertThat(memberId).isNotBlank().startsWith("group-1-");
    }

    @Test
    void joinGroup_withExistingMemberId_shouldReuseSuppliedId() {
        final var memberId = coordinator.joinGroup("group-1", "existing-member-id", new byte[0]);

        assertThat(memberId).isEqualTo("existing-member-id");
    }

    @Test
    void joinGroup_differentGroups_shouldBeIndependent() {
        final var memberId1 = coordinator.joinGroup("group-1", "", new byte[0]);
        final var memberId2 = coordinator.joinGroup("group-2", "", new byte[0]);

        assertThat(memberId1).isNotEqualTo(memberId2);
    }

    @Test
    void joinGroup_secondMember_shouldBeTrackedAlongsideFirstMember() {
        final var memberId1 = coordinator.joinGroup("group-1", "member-a", new byte[]{1});
        final var memberId2 = coordinator.joinGroup("group-1", "member-b", new byte[]{2});

        final var members = coordinator.getMembers("group-1");
        assertThat(members).containsKey(memberId1);
        assertThat(members).containsKey(memberId2);
    }

    @Test
    void joinGroup_firstMemberBecomesLeader() {
        coordinator.joinGroup("group-1", "member-a", new byte[0]);
        coordinator.joinGroup("group-1", "member-b", new byte[0]);

        assertThat(coordinator.getLeader("group-1")).isEqualTo("member-a");
    }

    @Test
    void joinGroup_storesSubscriptionMetadata() {
        final var metadata = new byte[]{10, 20, 30};
        coordinator.joinGroup("group-1", "member-1", metadata);

        assertThat(coordinator.getMembers("group-1").get("member-1")).isEqualTo(metadata);
    }

    @Test
    void getGenerationId_forKnownGroup_shouldReturnOne() {
        coordinator.joinGroup("group-1", "member-1", new byte[0]);

        assertThat(coordinator.getGenerationId("group-1")).isEqualTo(1);
    }

    @Test
    void getGenerationId_forUnknownGroup_shouldReturnOne() {
        assertThat(coordinator.getGenerationId("unknown-group")).isEqualTo(1);
    }

    @Test
    void getMembers_forUnknownGroup_shouldReturnEmptyMap() {
        assertThat(coordinator.getMembers("unknown-group")).isEmpty();
    }

    @Test
    void getLeader_forUnknownGroup_shouldReturnEmptyString() {
        assertThat(coordinator.getLeader("unknown-group")).isEmpty();
    }

    @Test
    void syncGroup_shouldStoreAssignments() {
        coordinator.joinGroup("group-1", "member-1", new byte[0]);
        final var assignmentBytes = new byte[]{1, 2, 3};
        coordinator.syncGroup("group-1", of("member-1", assignmentBytes));

        assertThat(coordinator.getMemberAssignment("group-1", "member-1"))
            .isEqualTo(assignmentBytes);
    }

    @Test
    void syncGroup_shouldPreserveMemberList() {
        coordinator.joinGroup("group-1", "member-a", new byte[]{1});
        coordinator.joinGroup("group-1", "member-b", new byte[]{2});
        coordinator.syncGroup("group-1", of(
            "member-a", new byte[]{10},
            "member-b", new byte[]{20}));

        assertThat(coordinator.getMembers("group-1")).containsKeys("member-a", "member-b");
    }

    @Test
    void syncGroup_forUnknownGroup_shouldNotFail() {
        // Should be a no-op rather than throwing
        coordinator.syncGroup("unknown-group", of("member-1", new byte[]{1}));
        // No assertion needed — just verifying no exception is thrown
    }

    @Test
    void getMemberAssignment_withNoSyncGroupCalled_shouldReturnEmptyArray() {
        coordinator.joinGroup("group-1", "member-1", new byte[0]);

        assertThat(coordinator.getMemberAssignment("group-1", "member-1")).isEmpty();
    }

    @Test
    void getMemberAssignment_forUnknownGroup_shouldReturnEmptyArray() {
        assertThat(coordinator.getMemberAssignment("unknown-group", "member-1")).isEmpty();
    }

    @Test
    void removeMember_shouldRemoveFromMemberList() {
        coordinator.joinGroup("group-1", "member-a", new byte[0]);
        coordinator.joinGroup("group-1", "member-b", new byte[0]);

        coordinator.removeMember("group-1", "member-b");

        assertThat(coordinator.getMembers("group-1")).containsOnlyKeys("member-a");
    }

    @Test
    void removeMember_lastMember_shouldRemoveGroupEntirely() {
        coordinator.joinGroup("group-1", "member-a", new byte[0]);

        coordinator.removeMember("group-1", "member-a");

        assertThat(coordinator.getMembers("group-1")).isEmpty();
        assertThat(coordinator.getLeader("group-1")).isEmpty();
    }

    @Test
    void removeMember_whenLeaderLeaves_shouldPromoteAnotherMember() {
        coordinator.joinGroup("group-1", "member-a", new byte[0]);
        coordinator.joinGroup("group-1", "member-b", new byte[0]);

        coordinator.removeMember("group-1", "member-a");

        assertThat(coordinator.getLeader("group-1")).isEqualTo("member-b");
    }

    @Test
    void removeMember_forUnknownGroup_shouldNotFail() {
        coordinator.removeMember("unknown-group", "member-1");
        // No assertion needed — just verifying no exception is thrown
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
    void deleteGroup_shouldRemoveGroupAndReturnTrue() {
        coordinator.joinGroup("group-1", "member-a", new byte[0]);
        coordinator.commitOffset("group-1", "topic-a", 0, 50L);

        final var result = coordinator.deleteGroup("group-1");

        assertThat(result).isTrue();
        assertThat(coordinator.getMembers("group-1")).isEmpty();
        assertThat(coordinator.getLeader("group-1")).isEmpty();
        assertThat(coordinator.getCommittedOffset("group-1", "topic-a", 0)).isEqualTo(-1L);
    }

    @Test
    void deleteGroup_forNonExistentGroup_shouldReturnFalse() {
        final var result = coordinator.deleteGroup("unknown-group");

        assertThat(result).isFalse();
    }

    @Test
    void deleteGroup_shouldNotAffectOtherGroups() {
        coordinator.joinGroup("group-1", "member-a", new byte[0]);
        coordinator.commitOffset("group-1", "topic-a", 0, 10L);
        coordinator.joinGroup("group-2", "member-b", new byte[0]);
        coordinator.commitOffset("group-2", "topic-a", 0, 20L);

        coordinator.deleteGroup("group-1");

        assertThat(coordinator.getMembers("group-2")).containsKey("member-b");
        assertThat(coordinator.getCommittedOffset("group-2", "topic-a", 0)).isEqualTo(20L);
    }

    @Test
    void deleteGroup_deletedGroupCanBeRejoined() {
        coordinator.joinGroup("group-1", "member-a", new byte[0]);
        coordinator.deleteGroup("group-1");

        final var memberId = coordinator.joinGroup("group-1", "member-b", new byte[0]);

        assertThat(memberId).isEqualTo("member-b");
        assertThat(coordinator.getMembers("group-1")).containsKey("member-b");
    }

    @Test
    void clear_shouldRemoveAllGroupStateAndOffsets() {
        coordinator.joinGroup("group-1", "member-1", new byte[0]);
        coordinator.commitOffset("group-1", "topic-a", 0, 50L);

        coordinator.clear();

        assertThat(coordinator.getCommittedOffset("group-1", "topic-a", 0)).isEqualTo(-1L);
        assertThat(coordinator.getMemberAssignment("group-1", "member-1")).isEmpty();
        assertThat(coordinator.getMembers("group-1")).isEmpty();
    }
}
