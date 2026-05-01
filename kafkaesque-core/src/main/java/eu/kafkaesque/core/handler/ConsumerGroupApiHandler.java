package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.connection.ClientConnection;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SyncGroupRequest;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static eu.kafkaesque.core.handler.ResponseSerializer.serialize;
import static java.util.List.of;

/**
 * Handles Kafka consumer group lifecycle API responses.
 *
 * <p>Covers {@link ApiKeys#JOIN_GROUP}, {@link ApiKeys#SYNC_GROUP},
 * {@link ApiKeys#HEARTBEAT}, and {@link ApiKeys#LEAVE_GROUP}.</p>
 *
 * <p>JoinGroup responses are <em>deferred</em>: when a member joins, the handler
 * waits up to {@link #REBALANCE_WINDOW_MS} milliseconds for other members of the
 * same group to arrive before responding to all of them together. This ensures that
 * every member in a multi-member group participates in the same rebalance generation
 * and that the elected leader receives the complete member list it needs to compute
 * partition assignments.</p>
 *
 * @see KafkaProtocolHandler
 * @see GroupCoordinator
 */
@Slf4j
@RequiredArgsConstructor
final class ConsumerGroupApiHandler {

    /**
     * Time to wait (in milliseconds) from the first {@code JoinGroup} request before
     * completing the rebalance and responding to all pending members.
     *
     * <p>All group members that arrive within this window are included in the same
     * rebalance generation. Members that arrive after the window will trigger a new
     * rebalance.</p>
     */
    static final long REBALANCE_WINDOW_MS = 200L;

    /**
     * Maximum time (in milliseconds) to wait for the leader to sync before
     * responding with an empty assignment.
     */
    private static final long SYNC_TIMEOUT_MS = 500L;

    /** Pending JoinGroup entry for a single member awaiting rebalance completion. */
    @EqualsAndHashCode
    @ToString
    private static final class PendingJoin {

        /** The member ID assigned by the group coordinator. */
        private final String memberId;

        /** The subscription metadata from the join protocol. */
        private final byte[] subscriptionMetadata;

        /** The protocol name from the join protocol. */
        private final String protocolName;

        /** The request header for sending the deferred response. */
        private final RequestHeader requestHeader;

        /** The client connection for this member. */
        private final ClientConnection connection;

        /** The NIO selection key for this connection. */
        private final SelectionKey selectionKey;

        /**
         * Creates a new pending join entry.
         *
         * @param memberId               the member ID
         * @param subscriptionMetadata   the subscription metadata
         * @param protocolName           the protocol name
         * @param requestHeader          the request header
         * @param connection             the client connection
         * @param selectionKey           the NIO selection key
         */
        PendingJoin(final String memberId, final byte[] subscriptionMetadata,
                    final String protocolName, final RequestHeader requestHeader,
                    final ClientConnection connection, final SelectionKey selectionKey) {
            this.memberId = memberId;
            this.subscriptionMetadata = subscriptionMetadata;
            this.protocolName = protocolName;
            this.requestHeader = requestHeader;
            this.connection = connection;
            this.selectionKey = selectionKey;
        }

        /**
         * Returns the member ID.
         *
         * @return the member ID
         */
        String memberId() {
            return memberId;
        }

        /**
         * Returns the subscription metadata.
         *
         * @return the subscription metadata
         */
        byte[] subscriptionMetadata() {
            return subscriptionMetadata;
        }

        /**
         * Returns the protocol name.
         *
         * @return the protocol name
         */
        String protocolName() {
            return protocolName;
        }

        /**
         * Returns the request header.
         *
         * @return the request header
         */
        RequestHeader requestHeader() {
            return requestHeader;
        }

        /**
         * Returns the client connection.
         *
         * @return the client connection
         */
        ClientConnection connection() {
            return connection;
        }

        /**
         * Returns the NIO selection key.
         *
         * @return the selection key
         */
        SelectionKey selectionKey() {
            return selectionKey;
        }
    }

    /** Pending SyncGroup entry for a follower awaiting the leader's assignment. */
    @EqualsAndHashCode
    @ToString
    private static final class PendingSync {

        /** The group ID. */
        private final String groupId;

        /** The member ID. */
        private final String memberId;

        /** The protocol name from the request. */
        private final String protocolName;

        /** The request header for sending the deferred response. */
        private final RequestHeader requestHeader;

        /** The client connection for this member. */
        private final ClientConnection connection;

        /** The NIO selection key for this connection. */
        private final SelectionKey selectionKey;

        /**
         * Creates a new pending sync entry.
         *
         * @param groupId        the group ID
         * @param memberId       the member ID
         * @param protocolName   the protocol name
         * @param requestHeader  the request header
         * @param connection     the client connection
         * @param selectionKey   the NIO selection key
         */
        PendingSync(final String groupId, final String memberId,
                    final String protocolName, final RequestHeader requestHeader,
                    final ClientConnection connection, final SelectionKey selectionKey) {
            this.groupId = groupId;
            this.memberId = memberId;
            this.protocolName = protocolName;
            this.requestHeader = requestHeader;
            this.connection = connection;
            this.selectionKey = selectionKey;
        }

        /**
         * Returns the group ID.
         *
         * @return the group ID
         */
        String groupId() {
            return groupId;
        }

        /**
         * Returns the member ID.
         *
         * @return the member ID
         */
        String memberId() {
            return memberId;
        }

        /**
         * Returns the protocol name.
         *
         * @return the protocol name
         */
        String protocolName() {
            return protocolName;
        }

        /**
         * Returns the request header.
         *
         * @return the request header
         */
        RequestHeader requestHeader() {
            return requestHeader;
        }

        /**
         * Returns the client connection.
         *
         * @return the client connection
         */
        ClientConnection connection() {
            return connection;
        }

        /**
         * Returns the NIO selection key.
         *
         * @return the selection key
         */
        SelectionKey selectionKey() {
            return selectionKey;
        }
    }

    private final GroupCoordinator groupCoordinator;

    /** Callback invoked to deliver each deferred response to the NIO event loop. */
    private final Consumer<DeferredResponse> responseDispatcher;

    /** Pending joins per group ID, populated as JoinGroup requests arrive. */
    private final Map<String, List<PendingJoin>> pendingJoins = new ConcurrentHashMap<>();

    /** Pending follower syncs per group ID, awaiting the leader's assignment. */
    private final Map<String, List<PendingSync>> pendingSyncs = new ConcurrentHashMap<>();

    /** One outstanding rebalance timer per group. */
    private final Map<String, ScheduledFuture<?>> rebalanceTimers = new ConcurrentHashMap<>();

    /** Single-thread scheduler for rebalance timers; uses a daemon thread so it does not block shutdown. */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
        r -> {
            final var t = new Thread(r, "kafkaesque-rebalance");
            t.setDaemon(true);
            return t;
        });

    /**
     * Processes a JOIN_GROUP request by registering the member and deferring the response
     * until the rebalance window closes.
     *
     * <p>Returns {@code null} to signal to the caller that no response should be written
     * immediately; the response will be dispatched via {@link #responseDispatcher} once all
     * members in the same group have joined (or the window expires).</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed JOIN_GROUP request
     * @param connection    the client connection for this member
     * @param selectionKey  the NIO selection key for this connection
     * @return always {@code null} — response is deferred
     */
    ByteBuffer generateJoinGroupResponse(
            final RequestHeader requestHeader,
            final JoinGroupRequest request,
            final ClientConnection connection,
            final SelectionKey selectionKey) {
        final var data = request.data();

        final var protocol = data.protocols().iterator().next();
        final var memberId = groupCoordinator.joinGroup(
            data.groupId(), data.memberId(), protocol.metadata());

        final var pending = new PendingJoin(
            memberId,
            protocol.metadata(),
            protocol.name(),
            requestHeader,
            connection,
            selectionKey);

        pendingJoins.computeIfAbsent(data.groupId(), k -> new ArrayList<>()).add(pending);

        // Schedule the rebalance-completion timer only once per group rebalance window.
        rebalanceTimers.computeIfAbsent(data.groupId(), gid ->
            scheduler.schedule(() -> completeRebalance(gid), REBALANCE_WINDOW_MS, TimeUnit.MILLISECONDS));

        log.debug("JoinGroup deferred: group={}, member={}", data.groupId(), memberId);

        return null;
    }

    /**
     * Completes a group rebalance by sending JoinGroup responses to all pending members.
     *
     * <p>Called by the rebalance timer. The group leader receives the full member list so
     * it can compute partition assignments; followers receive an empty list.</p>
     *
     * @param groupId the group whose rebalance window has closed
     */
    private void completeRebalance(final String groupId) {
        try {
            final var joins = pendingJoins.remove(groupId);
            rebalanceTimers.remove(groupId);

            if (joins == null || joins.isEmpty()) {
                return;
            }

            final var leader = electLeaderForRebalance(groupId, joins);
            final var generationId = groupCoordinator.incrementGeneration(groupId);
            final var protocolName = joins.get(0).protocolName();

            final var memberList = joins.stream()
                .map(j -> new JoinGroupResponseData.JoinGroupResponseMember()
                    .setMemberId(j.memberId())
                    .setMetadata(j.subscriptionMetadata()))
                .collect(Collectors.toList());

            log.debug("Completing rebalance: group={}, generation={}, leader={}, members={}",
                groupId, generationId, leader, joins.size());

            for (final var join : joins) {
                if (!join.selectionKey().isValid()) {
                    log.debug("Skipping deferred response — connection closed: group={}, member={}",
                        groupId, join.memberId());
                    continue;
                }
                final var isLeader = join.memberId().equals(leader);
                final var data = new JoinGroupResponseData()
                    .setErrorCode((short) 0)
                    .setGenerationId(generationId)
                    .setProtocolType("consumer")
                    .setProtocolName(protocolName)
                    .setLeader(leader)
                    .setSkipAssignment(false)
                    .setMemberId(join.memberId())
                    .setMembers(isLeader ? memberList : of());

                final var responseBuffer = serialize(
                    join.requestHeader(), data, ApiKeys.JOIN_GROUP);
                responseDispatcher.accept(
                    new DeferredResponse(join.connection(), join.selectionKey(), responseBuffer));
            }
        } catch (final Exception e) {
            log.error("Error completing rebalance for group {}", groupId, e);
        }
    }

    /**
     * Elects a leader for a rebalance from the pending members. If the current leader
     * is among the pending members it is retained; otherwise the first pending member
     * is promoted.
     *
     * @param groupId the consumer group ID
     * @param joins   the pending join entries
     * @return the elected leader member ID
     */
    private String electLeaderForRebalance(final String groupId, final List<PendingJoin> joins) {
        final var pendingMemberIds = joins.stream()
            .map(PendingJoin::memberId)
            .collect(Collectors.toSet());
        final var currentLeader = groupCoordinator.getLeader(groupId);
        final var leader = pendingMemberIds.contains(currentLeader)
            ? currentLeader
            : joins.get(0).memberId();
        if (!leader.equals(currentLeader)) {
            groupCoordinator.setLeader(groupId, leader);
        }
        return leader;
    }

    /**
     * Generates a SYNC_GROUP response, storing the leader's assignments and returning
     * this member's assignment.
     *
     * <p>When the leader sends its assignments, any pending follower syncs for the same
     * group are completed immediately. When a follower arrives before the leader, its
     * response is deferred until the leader syncs or a timeout expires.</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed SYNC_GROUP request
     * @param connection    the client connection for this member
     * @param selectionKey  the NIO selection key for this connection
     * @return the serialised response buffer, or null when the response is deferred
     */
    ByteBuffer generateSyncGroupResponse(
            final RequestHeader requestHeader, final SyncGroupRequest request,
            final ClientConnection connection, final SelectionKey selectionKey) {
        final var data = request.data();
        final var protocolName = data.protocolName() != null ? data.protocolName() : "range";

        if (!data.assignments().isEmpty()) {
            final var assignments = data.assignments().stream()
                .collect(Collectors.toMap(
                    SyncGroupRequestData.SyncGroupRequestAssignment::memberId,
                    SyncGroupRequestData.SyncGroupRequestAssignment::assignment));
            groupCoordinator.syncGroup(data.groupId(), assignments);
            completePendingSyncs(data.groupId());
        }

        final var assignment = groupCoordinator.getMemberAssignment(
            data.groupId(), data.memberId());

        final var isKnownFollower = assignment.length == 0
            && groupCoordinator.hasGroup(data.groupId())
            && groupCoordinator.getMemberCount(data.groupId()) > 1
            && !data.memberId().equals(groupCoordinator.getLeader(data.groupId()));
        if (isKnownFollower && selectionKey != null) {
            // Follower arrived before leader — defer until leader syncs or timeout
            pendingSyncs.computeIfAbsent(data.groupId(), k -> new ArrayList<>())
                .add(new PendingSync(
                    data.groupId(), data.memberId(), protocolName,
                    requestHeader, connection, selectionKey));
            scheduler.schedule(
                () -> completePendingSyncs(data.groupId()),
                SYNC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            log.debug("SyncGroup deferred: group={}, member={}", data.groupId(), data.memberId());
            return null;
        }

        log.debug("SyncGroup: group={}, member={}, assignmentBytes={}",
            data.groupId(), data.memberId(), assignment.length);

        return buildSyncGroupResponse(requestHeader, protocolName, assignment);
    }

    /**
     * Completes all pending follower syncs for a group by sending their assignments
     * via the deferred response mechanism.
     *
     * @param groupId the consumer group ID
     */
    private void completePendingSyncs(final String groupId) {
        final var syncs = pendingSyncs.remove(groupId);
        if (syncs == null || syncs.isEmpty()) {
            return;
        }
        for (final var sync : syncs) {
            if (!sync.selectionKey().isValid()) {
                log.debug("Skipping deferred sync — connection closed: group={}, member={}",
                    groupId, sync.memberId());
                continue;
            }
            final var assignment = groupCoordinator.getMemberAssignment(groupId, sync.memberId());
            final var responseBuffer = buildSyncGroupResponse(
                sync.requestHeader(), sync.protocolName(), assignment);
            responseDispatcher.accept(
                new DeferredResponse(sync.connection(), sync.selectionKey(), responseBuffer));
        }
    }

    /**
     * Builds a serialised SYNC_GROUP response.
     *
     * @param requestHeader the original request header
     * @param protocolName  the protocol name
     * @param assignment    the serialised partition assignment bytes
     * @return the serialised response buffer
     */
    private static ByteBuffer buildSyncGroupResponse(
            final RequestHeader requestHeader, final String protocolName, final byte[] assignment) {
        final var data = new SyncGroupResponseData()
            .setErrorCode((short) 0)
            .setProtocolType("consumer")
            .setProtocolName(protocolName)
            .setAssignment(assignment);
        return serialize(requestHeader, data, ApiKeys.SYNC_GROUP);
    }

    /**
     * Generates a HEARTBEAT response acknowledging consumer liveness.
     *
     * @param requestHeader the request header
     * @param request       the parsed HEARTBEAT request
     * @return the serialised response buffer
     */
    ByteBuffer generateHeartbeatResponse(final RequestHeader requestHeader, final HeartbeatRequest request) {
        final var data = request.data();
        log.debug("Heartbeat: group={}, member={}", data.groupId(), data.memberId());

        return serialize(requestHeader,
            new HeartbeatResponseData().setThrottleTimeMs(0).setErrorCode((short) 0),
            ApiKeys.HEARTBEAT);
    }

    /**
     * Generates a LEAVE_GROUP response acknowledging consumer departure and removes
     * the departing members from the group.
     *
     * @param requestHeader the request header
     * @param request       the parsed LEAVE_GROUP request
     * @return the serialised response buffer
     */
    ByteBuffer generateLeaveGroupResponse(final RequestHeader requestHeader, final LeaveGroupRequest request) {
        final var data = request.data();

        for (final var member : data.members()) {
            groupCoordinator.removeMember(data.groupId(), member.memberId());
        }

        log.debug("LeaveGroup: group={}", data.groupId());

        return serialize(requestHeader,
            new LeaveGroupResponseData().setThrottleTimeMs(0).setErrorCode((short) 0),
            ApiKeys.LEAVE_GROUP);
    }

    /**
     * Generates a LIST_GROUPS response listing all known consumer groups.
     *
     * <p>The {@code request} parameter is part of the dispatcher contract but the listing is
     * unfiltered; all known groups are returned regardless of the request's optional state
     * filters.</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed LIST_GROUPS request (currently unused)
     * @return the serialised response buffer
     */
    ByteBuffer generateListGroupsResponse(final RequestHeader requestHeader, final ListGroupsRequest request) {
        final var groups = groupCoordinator.getGroupIds().stream()
            .map(gid -> new ListGroupsResponseData.ListedGroup()
                .setGroupId(gid)
                .setProtocolType("consumer")
                .setGroupState("Stable")
                .setGroupType("consumer"))
            .collect(Collectors.toList());

        final var response = new ListGroupsResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode((short) 0)
            .setGroups(groups);

        return serialize(requestHeader, response, ApiKeys.LIST_GROUPS);
    }

    /**
     * Generates a CONSUMER_GROUP_DESCRIBE response with member details for the
     * requested consumer groups.
     *
     * @param requestHeader the request header
     * @param request       the parsed CONSUMER_GROUP_DESCRIBE request
     * @return the serialised response buffer
     */
    ByteBuffer generateConsumerGroupDescribeResponse(
            final RequestHeader requestHeader, final ConsumerGroupDescribeRequest request) {
        final var data = request.data();

        final var groups = data.groupIds().stream()
            .map(gid -> {
                final var members = groupCoordinator.getMembers(gid).keySet().stream()
                    .map(s -> new ConsumerGroupDescribeResponseData.Member()
                            .setMemberId(s)
                            .setClientId("")
                            .setClientHost(""))
                    .collect(Collectors.toList());
                return new ConsumerGroupDescribeResponseData.DescribedGroup()
                    .setGroupId(gid)
                    .setErrorCode((short) 0)
                    .setGroupState("Stable")
                    .setGroupEpoch(groupCoordinator.getGenerationId(gid))
                    .setAssignmentEpoch(groupCoordinator.getGenerationId(gid))
                    .setAssignorName("range")
                    .setMembers(members)
                    .setAuthorizedOperations(-2147483648);
            })
            .collect(Collectors.toList());

        final var response = new ConsumerGroupDescribeResponseData()
            .setThrottleTimeMs(0)
            .setGroups(groups);

        return serialize(
            requestHeader, response, ApiKeys.CONSUMER_GROUP_DESCRIBE);
    }

    /**
     * Generates a DELETE_GROUPS response after removing the requested consumer groups.
     *
     * @param requestHeader the request header
     * @param request       the parsed DELETE_GROUPS request
     * @return the serialised response buffer
     */
    ByteBuffer generateDeleteGroupsResponse(final RequestHeader requestHeader, final DeleteGroupsRequest request) {
        final var data = request.data();

        final var results = new DeleteGroupsResponseData.DeletableGroupResultCollection();
        data.groupsNames().stream()
            .map(gid -> {
                groupCoordinator.deleteGroup(gid);
                log.info("Deleted consumer group: {}", gid);
                return new DeleteGroupsResponseData.DeletableGroupResult()
                    .setGroupId(gid)
                    .setErrorCode((short) 0);
            })
            .forEach(results::add);

        final var response = new DeleteGroupsResponseData()
            .setThrottleTimeMs(0)
            .setResults(results);

        return serialize(requestHeader, response, ApiKeys.DELETE_GROUPS);
    }

    /**
     * Generates a DESCRIBE_GROUPS response with member details for the requested groups.
     *
     * <p>This handles the legacy {@link ApiKeys#DESCRIBE_GROUPS} API (key 15), used by
     * older versions of the Kafka AdminClient before the newer
     * {@link ApiKeys#CONSUMER_GROUP_DESCRIBE} (key 69) was introduced.</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed DESCRIBE_GROUPS request
     * @return the serialised response buffer
     */
    ByteBuffer generateDescribeGroupsResponse(
            final RequestHeader requestHeader, final DescribeGroupsRequest request) {
        final var data = request.data();

        final var groups = data.groups().stream()
            .map(this::describeGroup)
            .collect(Collectors.toList());

        final var response = new DescribeGroupsResponseData()
            .setThrottleTimeMs(0)
            .setGroups(groups);

        return serialize(requestHeader, response, ApiKeys.DESCRIBE_GROUPS);
    }

    /**
     * Builds a single described group entry for the DESCRIBE_GROUPS response.
     *
     * @param groupId the group ID to describe
     * @return the described group entry
     */
    private DescribeGroupsResponseData.DescribedGroup describeGroup(final String groupId) {
        final var members = groupCoordinator.getMembers(groupId).entrySet().stream()
            .map(entry -> new DescribeGroupsResponseData.DescribedGroupMember()
                .setMemberId(entry.getKey())
                .setClientId("")
                .setClientHost("")
                .setMemberMetadata(entry.getValue())
                .setMemberAssignment(
                    groupCoordinator.getMemberAssignment(groupId, entry.getKey())))
            .collect(Collectors.toList());
        return new DescribeGroupsResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode((short) 0)
            .setGroupState(groupCoordinator.hasGroup(groupId) ? "Stable" : "Dead")
            .setProtocolType("consumer")
            .setProtocolData("range")
            .setMembers(members);
    }

    /**
     * Shuts down the rebalance timer scheduler, releasing its backing thread.
     */
    void close() {
        scheduler.shutdownNow();
    }
}
