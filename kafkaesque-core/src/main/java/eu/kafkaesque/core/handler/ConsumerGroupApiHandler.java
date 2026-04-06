package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.connection.ClientConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.RequestHeader;

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

    /** Pending JoinGroup entry for a single member awaiting rebalance completion. */
    private record PendingJoin(
        String memberId,
        byte[] subscriptionMetadata,
        String protocolName,
        RequestHeader requestHeader,
        ClientConnection connection,
        SelectionKey selectionKey
    ) {}

    private final GroupCoordinator groupCoordinator;

    /** Callback invoked to deliver each deferred JoinGroup response to the NIO event loop. */
    private final Consumer<DeferredResponse> responseDispatcher;

    /** Pending joins per group ID, populated as JoinGroup requests arrive. */
    private final Map<String, List<PendingJoin>> pendingJoins = new ConcurrentHashMap<>();

    /** One outstanding rebalance timer per group. */
    private final Map<String, ScheduledFuture<?>> rebalanceTimers = new ConcurrentHashMap<>();

    /** Single-thread scheduler for rebalance timers; uses a daemon thread so it does not block shutdown. */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
        r -> Thread.ofPlatform().daemon(true).name("kafkaesque-rebalance").unstarted(r));

    /**
     * Creates a new handler backed by the given group coordinator.
     *
     * @param groupCoordinator   the coordinator managing group membership and assignments
     * @param responseDispatcher callback used to enqueue deferred JoinGroup responses for
     *                           delivery by the NIO event loop
     */
    ConsumerGroupApiHandler(
            final GroupCoordinator groupCoordinator,
            final Consumer<DeferredResponse> responseDispatcher) {
        this.groupCoordinator = groupCoordinator;
        this.responseDispatcher = responseDispatcher;
    }

    /**
     * Processes a JOIN_GROUP request by registering the member and deferring the response
     * until the rebalance window closes.
     *
     * <p>Returns {@code null} to signal to the caller that no response should be written
     * immediately; the response will be dispatched via {@link #responseDispatcher} once all
     * members in the same group have joined (or the window expires).</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @param connection    the client connection for this member
     * @param selectionKey  the NIO selection key for this connection
     * @return always {@code null} — response is deferred
     */
    ByteBuffer generateJoinGroupResponse(
            final RequestHeader requestHeader,
            final ByteBuffer buffer,
            final ClientConnection connection,
            final SelectionKey selectionKey) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new JoinGroupRequestData(accessor, requestHeader.apiVersion());

            final var protocol = request.protocols().iterator().next();
            final var memberId = groupCoordinator.joinGroup(
                request.groupId(), request.memberId(), protocol.metadata());

            final var pending = new PendingJoin(
                memberId,
                protocol.metadata(),
                protocol.name(),
                requestHeader,
                connection,
                selectionKey);

            pendingJoins.computeIfAbsent(request.groupId(), k -> new ArrayList<>()).add(pending);

            // Schedule the rebalance-completion timer only once per group rebalance window.
            rebalanceTimers.computeIfAbsent(request.groupId(), gid ->
                scheduler.schedule(() -> completeRebalance(gid), REBALANCE_WINDOW_MS, TimeUnit.MILLISECONDS));

            log.debug("JoinGroup deferred: group={}, member={}", request.groupId(), memberId);

        } catch (final Exception e) {
            log.error("Error processing JoinGroup request", e);
        }
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

            final var leader = groupCoordinator.getLeader(groupId);
            final var generationId = groupCoordinator.getGenerationId(groupId);
            final var protocolName = joins.get(0).protocolName();

            final var memberList = joins.stream()
                .map(j -> new JoinGroupResponseData.JoinGroupResponseMember()
                    .setMemberId(j.memberId())
                    .setMetadata(j.subscriptionMetadata()))
                .toList();

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

                final var responseBuffer = ResponseSerializer.serialize(
                    join.requestHeader(), data, ApiKeys.JOIN_GROUP);
                if (responseBuffer != null) {
                    responseDispatcher.accept(
                        new DeferredResponse(join.connection(), join.selectionKey(), responseBuffer));
                }
            }
        } catch (final Exception e) {
            log.error("Error completing rebalance for group {}", groupId, e);
        }
    }

    /**
     * Generates a SYNC_GROUP response, storing the leader's assignments and returning
     * this member's assignment.
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateSyncGroupResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new SyncGroupRequestData(accessor, requestHeader.apiVersion());

            if (!request.assignments().isEmpty()) {
                final var assignments = request.assignments().stream()
                    .collect(Collectors.toMap(
                        SyncGroupRequestData.SyncGroupRequestAssignment::memberId,
                        SyncGroupRequestData.SyncGroupRequestAssignment::assignment));
                groupCoordinator.syncGroup(request.groupId(), assignments);
            }

            final var assignment = groupCoordinator.getMemberAssignment(request.groupId(), request.memberId());
            final var protocolName = request.protocolName() != null ? request.protocolName() : "range";

            final var data = new SyncGroupResponseData()
                .setErrorCode((short) 0)
                .setProtocolType("consumer")
                .setProtocolName(protocolName)
                .setAssignment(assignment);

            log.debug("SyncGroup: group={}, member={}, assignmentBytes={}",
                request.groupId(), request.memberId(), assignment.length);

            return ResponseSerializer.serialize(requestHeader, data, ApiKeys.SYNC_GROUP);

        } catch (final Exception e) {
            log.error("Error generating SyncGroup response", e);
            return null;
        }
    }

    /**
     * Generates a HEARTBEAT response acknowledging consumer liveness.
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateHeartbeatResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new HeartbeatRequestData(accessor, requestHeader.apiVersion());
            log.debug("Heartbeat: group={}, member={}", request.groupId(), request.memberId());

            return ResponseSerializer.serialize(requestHeader,
                new HeartbeatResponseData().setThrottleTimeMs(0).setErrorCode((short) 0),
                ApiKeys.HEARTBEAT);

        } catch (final Exception e) {
            log.error("Error generating Heartbeat response", e);
            return null;
        }
    }

    /**
     * Generates a LEAVE_GROUP response acknowledging consumer departure and removes
     * the departing members from the group.
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateLeaveGroupResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new LeaveGroupRequestData(accessor, requestHeader.apiVersion());

            for (final var member : request.members()) {
                groupCoordinator.removeMember(request.groupId(), member.memberId());
            }

            log.debug("LeaveGroup: group={}", request.groupId());

            return ResponseSerializer.serialize(requestHeader,
                new LeaveGroupResponseData().setThrottleTimeMs(0).setErrorCode((short) 0),
                ApiKeys.LEAVE_GROUP);

        } catch (final Exception e) {
            log.error("Error generating LeaveGroup response", e);
            return null;
        }
    }

    /**
     * Shuts down the rebalance timer scheduler, releasing its backing thread.
     */
    void close() {
        scheduler.shutdownNow();
    }
}
