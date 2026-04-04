package eu.kafkaesque.core.handler;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages consumer group membership, partition assignments, and committed offsets
 * for the Kafkaesque mock server.
 *
 * <p>A single active member per group is supported, which is sufficient for the
 * single-consumer test scenarios Kafkaesque targets. The joining member is always
 * designated as the group leader so it can compute and submit its own partition
 * assignments during the SYNC_GROUP phase.</p>
 *
 * <p>This class is thread-safe.</p>
 */
@Slf4j
public final class GroupCoordinator {

    /** Identifies a single partition within a topic. */
    private record TopicPartitionKey(String topic, int partition) {}

    /** Immutable snapshot of a consumer group's current state. */
    private record GroupState(String memberId, int generationId, Map<String, byte[]> assignments) {}

    /** Active group states keyed by group ID. */
    private final Map<String, GroupState> groups = new ConcurrentHashMap<>();

    /** Committed offsets: groupId → (topic, partition) → committed offset. */
    private final Map<String, Map<TopicPartitionKey, Long>> committedOffsets = new ConcurrentHashMap<>();

    /**
     * Records a member joining a consumer group.
     *
     * <p>If {@code requestedMemberId} is blank a unique ID is generated, otherwise
     * the supplied ID is reused (re-join after rebalance). The joining member is
     * always designated the group leader.</p>
     *
     * @param groupId           the consumer group ID
     * @param requestedMemberId the member ID proposed by the client; may be blank for new members
     * @return the member ID assigned by this coordinator
     */
    public String joinGroup(final String groupId, final String requestedMemberId) {
        final var memberId = (requestedMemberId == null || requestedMemberId.isBlank())
            ? groupId + "-" + UUID.randomUUID()
            : requestedMemberId;
        groups.put(groupId, new GroupState(memberId, 1, new ConcurrentHashMap<>()));
        log.debug("Member {} joined group {}", memberId, groupId);
        return memberId;
    }

    /**
     * Stores the partition assignments submitted by the group leader during SYNC_GROUP.
     *
     * @param groupId     the consumer group ID
     * @param assignments map from member ID to the serialised partition assignment bytes
     */
    public void syncGroup(final String groupId, final Map<String, byte[]> assignments) {
        final var existing = groups.get(groupId);
        if (existing != null) {
            groups.put(groupId, new GroupState(existing.memberId(), existing.generationId(), assignments));
            log.debug("Synced group {} with {} assignment(s)", groupId, assignments.size());
        }
    }

    /**
     * Returns the current generation ID for a consumer group.
     *
     * @param groupId the consumer group ID
     * @return the generation ID, or {@code 1} if the group has not yet been created
     */
    public int getGenerationId(final String groupId) {
        final var state = groups.get(groupId);
        return state != null ? state.generationId() : 1;
    }

    /**
     * Returns the serialised partition assignment bytes for a specific group member.
     *
     * @param groupId  the consumer group ID
     * @param memberId the member ID
     * @return assignment bytes, or an empty array if no assignment has been stored yet
     */
    public byte[] getMemberAssignment(final String groupId, final String memberId) {
        final var state = groups.get(groupId);
        if (state != null) {
            final var assignment = state.assignments().get(memberId);
            if (assignment != null) {
                return assignment;
            }
        }
        return new byte[0];
    }

    /**
     * Returns the last committed offset for a topic-partition within a consumer group.
     *
     * @param groupId   the consumer group ID
     * @param topic     the topic name
     * @param partition the partition index
     * @return the committed offset, or {@code -1} if no offset has been committed yet
     */
    public long getCommittedOffset(final String groupId, final String topic, final int partition) {
        final var groupOffsets = committedOffsets.get(groupId);
        if (groupOffsets == null) {
            return -1L;
        }
        return groupOffsets.getOrDefault(new TopicPartitionKey(topic, partition), -1L);
    }

    /**
     * Records a committed offset for a topic-partition within a consumer group.
     *
     * @param groupId   the consumer group ID
     * @param topic     the topic name
     * @param partition the partition index
     * @param offset    the offset to commit
     */
    public void commitOffset(final String groupId, final String topic, final int partition, final long offset) {
        committedOffsets.computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
            .put(new TopicPartitionKey(topic, partition), offset);
        log.debug("Committed offset {} for group={}, topic={}, partition={}", offset, groupId, topic, partition);
    }

    /**
     * Clears all group state and committed offsets.
     */
    public void clear() {
        groups.clear();
        committedOffsets.clear();
    }
}
