package eu.kafkaesque.core.handler;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Map.copyOf;
import static java.util.Map.of;

/**
 * Manages consumer group membership, partition assignments, and committed offsets
 * for the Kafkaesque mock server.
 *
 * <p>Multiple members per group are supported. The first member to join a group is
 * designated as the leader so it can compute and submit partition assignments for all
 * members during the SYNC_GROUP phase.</p>
 *
 * <p>This class is thread-safe.</p>
 */
@Slf4j
public final class GroupCoordinator {

    /**
     * Identifies a single partition within a topic.
     */
    @EqualsAndHashCode
    @ToString
    private static final class TopicPartitionKey {

        /** The topic name. */
        private final String topic;

        /** The partition index. */
        private final int partition;

        /**
         * Creates a new {@code TopicPartitionKey}.
         *
         * @param topic     the topic name
         * @param partition the partition index
         */
        TopicPartitionKey(final String topic, final int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        /**
         * Returns the topic name.
         *
         * @return the topic name
         */
        String topic() {
            return topic;
        }

        /**
         * Returns the partition index.
         *
         * @return the partition index
         */
        int partition() {
            return partition;
        }
    }

    /**
     * Immutable snapshot of a consumer group's current state.
     */
    @EqualsAndHashCode
    @ToString
    private static final class GroupState {

        /** Map from member ID to serialised subscription metadata. */
        private final Map<String, byte[]> memberSubscriptions;

        /** Current rebalance generation. */
        private final int generationId;

        /** The member elected as group leader. */
        private final String leaderId;

        /** Map from member ID to serialised partition assignment bytes. */
        private final Map<String, byte[]> assignments;

        /**
         * Creates a new {@code GroupState}.
         *
         * @param memberSubscriptions map from member ID to serialised subscription metadata
         * @param generationId        current rebalance generation
         * @param leaderId            the member elected as group leader
         * @param assignments         map from member ID to serialised partition assignment bytes
         */
        GroupState(
                final Map<String, byte[]> memberSubscriptions,
                final int generationId,
                final String leaderId,
                final Map<String, byte[]> assignments) {
            this.memberSubscriptions = memberSubscriptions;
            this.generationId = generationId;
            this.leaderId = leaderId;
            this.assignments = assignments;
        }

        /**
         * Returns the member subscriptions map.
         *
         * @return map from member ID to serialised subscription metadata
         */
        Map<String, byte[]> memberSubscriptions() {
            return memberSubscriptions;
        }

        /**
         * Returns the current rebalance generation.
         *
         * @return the generation ID
         */
        int generationId() {
            return generationId;
        }

        /**
         * Returns the leader member ID.
         *
         * @return the leader member ID
         */
        String leaderId() {
            return leaderId;
        }

        /**
         * Returns the partition assignments map.
         *
         * @return map from member ID to serialised partition assignment bytes
         */
        Map<String, byte[]> assignments() {
            return assignments;
        }
    }

    /** Active group states keyed by group ID. */
    private final Map<String, GroupState> groups = new ConcurrentHashMap<>();

    /** Committed offsets: groupId -> (topic, partition) -> committed offset. */
    private final Map<String, Map<TopicPartitionKey, Long>> committedOffsets = new ConcurrentHashMap<>();

    /**
     * Records a member joining a consumer group.
     *
     * <p>If {@code requestedMemberId} is blank a unique ID is generated, otherwise
     * the supplied ID is reused (re-join after rebalance). The first member to join
     * a group is designated its leader.</p>
     *
     * @param groupId              the consumer group ID
     * @param requestedMemberId    the member ID proposed by the client; may be blank for new members
     * @param subscriptionMetadata the serialised subscription metadata from the JoinGroup request
     * @return the member ID assigned by this coordinator
     */
    public String joinGroup(
            final String groupId,
            final String requestedMemberId,
            final byte[] subscriptionMetadata) {
        final var memberId = (requestedMemberId == null || requestedMemberId.isBlank())
            ? groupId + "-" + UUID.randomUUID()
            : requestedMemberId;

        groups.compute(groupId, (gid, existing) -> {
            final var members = existing == null
                ? new ConcurrentHashMap<String, byte[]>()
                : new ConcurrentHashMap<>(existing.memberSubscriptions());
            members.put(memberId, subscriptionMetadata);
            final var leader = existing == null ? memberId : existing.leaderId();
            final var generationId = existing == null ? 1 : existing.generationId();
            final var assignments = existing == null
                ? new ConcurrentHashMap<String, byte[]>()
                : existing.assignments();
            return new GroupState(members, generationId, leader, assignments);
        });

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
            groups.put(groupId, new GroupState(
                existing.memberSubscriptions(),
                existing.generationId(),
                existing.leaderId(),
                assignments));
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
     * Returns all current members of a consumer group with their subscription metadata.
     *
     * @param groupId the consumer group ID
     * @return unmodifiable map from member ID to serialised subscription metadata;
     *         empty if the group is unknown
     */
    public Map<String, byte[]> getMembers(final String groupId) {
        final var state = groups.get(groupId);
        return state != null ? copyOf(state.memberSubscriptions()) : of();
    }

    /**
     * Returns the leader member ID for a consumer group.
     *
     * @param groupId the consumer group ID
     * @return the leader member ID, or an empty string if the group is unknown
     */
    public String getLeader(final String groupId) {
        final var state = groups.get(groupId);
        return state != null ? state.leaderId() : "";
    }

    /**
     * Updates the leader of a consumer group.
     *
     * <p>Used when a rebalance occurs and the previous leader is no longer among
     * the pending members.</p>
     *
     * @param groupId  the consumer group ID
     * @param leaderId the new leader member ID
     */
    public void setLeader(final String groupId, final String leaderId) {
        groups.computeIfPresent(groupId, (gid, existing) ->
            new GroupState(existing.memberSubscriptions(), existing.generationId(),
                leaderId, existing.assignments()));
    }

    /**
     * Increments the generation ID for a consumer group and clears any stale assignments.
     *
     * @param groupId the consumer group ID
     * @return the new generation ID
     */
    public int incrementGeneration(final String groupId) {
        final var state = groups.get(groupId);
        if (state == null) {
            return 1;
        }
        final var newGen = state.generationId() + 1;
        groups.put(groupId, new GroupState(
            state.memberSubscriptions(), newGen, state.leaderId(),
            new ConcurrentHashMap<>()));
        return newGen;
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
     * Removes a member from a consumer group.
     *
     * <p>If the group becomes empty after removal, its state is deleted entirely.</p>
     *
     * @param groupId  the consumer group ID
     * @param memberId the member ID to remove
     */
    public void removeMember(final String groupId, final String memberId) {
        groups.computeIfPresent(groupId, (gid, existing) -> {
            final var members = new ConcurrentHashMap<>(existing.memberSubscriptions());
            members.remove(memberId);
            if (members.isEmpty()) {
                return null;
            }
            final var newLeader = members.containsKey(existing.leaderId())
                ? existing.leaderId()
                : members.keySet().iterator().next();
            return new GroupState(members, existing.generationId(), newLeader, existing.assignments());
        });
        log.debug("Member {} left group {}", memberId, groupId);
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
     * Returns the IDs of all known consumer groups.
     *
     * @return unmodifiable set of group IDs
     */
    public Set<String> getGroupIds() {
        return Set.copyOf(groups.keySet());
    }

    /**
     * Returns whether a consumer group with the given ID exists.
     *
     * @param groupId the consumer group ID
     * @return {@code true} if the group is known
     */
    public boolean hasGroup(final String groupId) {
        return groups.containsKey(groupId);
    }

    /**
     * Returns the number of members currently in a consumer group.
     *
     * @param groupId the consumer group ID
     * @return the member count, or {@code 0} if the group is unknown
     */
    public int getMemberCount(final String groupId) {
        final var state = groups.get(groupId);
        return state != null ? state.memberSubscriptions().size() : 0;
    }

    /**
     * A committed offset entry for a specific topic and partition.
     */
    @EqualsAndHashCode
    @ToString
    public static final class CommittedOffsetEntry {

        /** The topic name. */
        private final String topic;

        /** The partition index. */
        private final int partition;

        /** The committed offset. */
        private final long offset;

        /**
         * Creates a new {@code CommittedOffsetEntry}.
         *
         * @param topic     the topic name
         * @param partition the partition index
         * @param offset    the committed offset
         */
        public CommittedOffsetEntry(final String topic, final int partition, final long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        /**
         * Returns the topic name.
         *
         * @return the topic name
         */
        public String topic() {
            return topic;
        }

        /**
         * Returns the partition index.
         *
         * @return the partition index
         */
        public int partition() {
            return partition;
        }

        /**
         * Returns the committed offset.
         *
         * @return the committed offset
         */
        public long offset() {
            return offset;
        }
    }

    /**
     * Returns all committed offsets for a consumer group.
     *
     * @param groupId the consumer group ID
     * @return list of committed offset entries; empty if the group has no committed offsets
     */
    public java.util.List<CommittedOffsetEntry> getCommittedOffsets(final String groupId) {
        final var groupOffsets = committedOffsets.get(groupId);
        if (groupOffsets == null) {
            return java.util.List.of();
        }
        return groupOffsets.entrySet().stream()
            .map(e -> new CommittedOffsetEntry(e.getKey().topic(), e.getKey().partition(), e.getValue()))
            .collect(Collectors.toList());
    }

    /**
     * Deletes a consumer group and all its committed offsets.
     *
     * <p>If the group does not exist, this method does nothing.</p>
     *
     * @param groupId the consumer group ID to delete
     * @return {@code true} if the group existed and was deleted
     */
    public boolean deleteGroup(final String groupId) {
        final var removed = groups.remove(groupId) != null;
        committedOffsets.remove(groupId);
        if (removed) {
            log.debug("Deleted group {}", groupId);
        }
        return removed;
    }

    /**
     * Clears all group state and committed offsets.
     */
    public void clear() {
        groups.clear();
        committedOffsets.clear();
    }
}
