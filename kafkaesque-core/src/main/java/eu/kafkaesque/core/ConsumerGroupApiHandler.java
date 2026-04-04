package eu.kafkaesque.core;

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
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handles Kafka consumer group lifecycle API responses.
 *
 * <p>Covers {@link ApiKeys#JOIN_GROUP}, {@link ApiKeys#SYNC_GROUP},
 * {@link ApiKeys#HEARTBEAT}, and {@link ApiKeys#LEAVE_GROUP}.</p>
 *
 * @see KafkaProtocolHandler
 * @see GroupCoordinator
 */
@Slf4j
final class ConsumerGroupApiHandler {

    private final GroupCoordinator groupCoordinator;

    /**
     * Creates a new handler backed by the given group coordinator.
     *
     * @param groupCoordinator the coordinator managing group membership and assignments
     */
    ConsumerGroupApiHandler(final GroupCoordinator groupCoordinator) {
        this.groupCoordinator = groupCoordinator;
    }

    /**
     * Generates a JOIN_GROUP response, designating the joining member as the sole leader.
     *
     * <p>Since only one member per group is supported, the joining member is always
     * leader and receives its own subscription metadata so it can compute partition
     * assignments to submit in SYNC_GROUP.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateJoinGroupResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new JoinGroupRequestData(accessor, requestHeader.apiVersion());

            final var memberId = groupCoordinator.joinGroup(request.groupId(), request.memberId());
            final var generationId = groupCoordinator.getGenerationId(request.groupId());

            final var protocol = request.protocols().iterator().next();

            final var member = new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId(memberId)
                .setMetadata(protocol.metadata());

            final var data = new JoinGroupResponseData()
                .setErrorCode((short) 0)
                .setGenerationId(generationId)
                .setProtocolType("consumer")
                .setProtocolName(protocol.name())
                .setLeader(memberId)
                .setSkipAssignment(false)
                .setMemberId(memberId)
                .setMembers(List.of(member));

            log.debug("JoinGroup: group={}, member={}, generation={}, protocol={}",
                request.groupId(), memberId, generationId, protocol.name());

            return ResponseSerializer.serialize(requestHeader, data, ApiKeys.JOIN_GROUP);

        } catch (final Exception e) {
            log.error("Error generating JoinGroup response", e);
            return null;
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
     * Generates a LEAVE_GROUP response acknowledging consumer departure.
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateLeaveGroupResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new LeaveGroupRequestData(accessor, requestHeader.apiVersion());
            log.debug("LeaveGroup: group={}", request.groupId());

            return ResponseSerializer.serialize(requestHeader,
                new LeaveGroupResponseData().setThrottleTimeMs(0).setErrorCode((short) 0),
                ApiKeys.LEAVE_GROUP);

        } catch (final Exception e) {
            log.error("Error generating LeaveGroup response", e);
            return null;
        }
    }
}
