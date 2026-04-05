package eu.kafkaesque.core.handler;

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
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ConsumerGroupApiHandler}.
 */
class ConsumerGroupApiHandlerTest {

    private GroupCoordinator groupCoordinator;
    private ConsumerGroupApiHandler handler;

    @BeforeEach
    void setUp() {
        groupCoordinator = new GroupCoordinator();
        handler = new ConsumerGroupApiHandler(groupCoordinator);
    }

    @Test
    void generateJoinGroupResponse_shouldReturnSuccessAndAssignMemberId() {
        final var apiVersion = ApiKeys.JOIN_GROUP.latestVersion();
        final var requestData = buildJoinGroupRequest("test-group", "");
        final var header = new RequestHeader(ApiKeys.JOIN_GROUP, apiVersion, "test-client", 1);

        final var response = handler.generateJoinGroupResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseJoinGroupResponse(response, apiVersion);
        assertThat(responseData.errorCode()).isZero();
        assertThat(responseData.memberId()).isNotBlank();
        assertThat(responseData.leader()).isEqualTo(responseData.memberId());
    }

    @Test
    void generateJoinGroupResponse_shouldRegisterMemberInCoordinator() {
        final var apiVersion = ApiKeys.JOIN_GROUP.latestVersion();
        final var requestData = buildJoinGroupRequest("test-group", "");
        final var header = new RequestHeader(ApiKeys.JOIN_GROUP, apiVersion, "test-client", 1);

        handler.generateJoinGroupResponse(header, serialize(requestData, apiVersion));

        assertThat(groupCoordinator.getGenerationId("test-group")).isEqualTo(1);
    }

    @Test
    void generateSyncGroupResponse_shouldReturnSuccessCode() {
        // First join the group
        final var joinApiVersion = ApiKeys.JOIN_GROUP.latestVersion();
        final var joinHeader = new RequestHeader(ApiKeys.JOIN_GROUP, joinApiVersion, "client", 1);
        handler.generateJoinGroupResponse(joinHeader, serialize(buildJoinGroupRequest("group-1", ""), joinApiVersion));

        // Then sync
        final var syncApiVersion = ApiKeys.SYNC_GROUP.latestVersion();
        final var syncRequestData = new SyncGroupRequestData()
            .setGroupId("group-1")
            .setGenerationId(1)
            .setMemberId("member-1")
            .setAssignments(List.of());
        final var syncHeader = new RequestHeader(ApiKeys.SYNC_GROUP, syncApiVersion, "client", 2);

        final var response = handler.generateSyncGroupResponse(
            syncHeader, serialize(syncRequestData, syncApiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseSyncGroupResponse(response, syncApiVersion);
        assertThat(responseData.errorCode()).isZero();
    }

    @Test
    void generateHeartbeatResponse_shouldReturnSuccessCode() {
        final var apiVersion = ApiKeys.HEARTBEAT.latestVersion();
        final var requestData = new HeartbeatRequestData()
            .setGroupId("my-group").setGenerationId(1).setMemberId("member-1");
        final var header = new RequestHeader(ApiKeys.HEARTBEAT, apiVersion, "test-client", 3);

        final var response = handler.generateHeartbeatResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseHeartbeatResponse(response, apiVersion);
        assertThat(responseData.errorCode()).isZero();
    }

    @Test
    void generateLeaveGroupResponse_shouldReturnSuccessCode() {
        final var apiVersion = ApiKeys.LEAVE_GROUP.latestVersion();
        final var requestData = new LeaveGroupRequestData()
            .setGroupId("my-group")
            .setMembers(List.of(new LeaveGroupRequestData.MemberIdentity().setMemberId("member-1")));
        final var header = new RequestHeader(ApiKeys.LEAVE_GROUP, apiVersion, "test-client", 4);

        final var response = handler.generateLeaveGroupResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseLeaveGroupResponse(response, apiVersion);
        assertThat(responseData.errorCode()).isZero();
    }

    // --- helpers ---

    private static JoinGroupRequestData buildJoinGroupRequest(final String groupId, final String memberId) {
        final var protocol = new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[0]);
        return new JoinGroupRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setSessionTimeoutMs(30000)
            .setRebalanceTimeoutMs(30000)
            .setProtocolType("consumer")
            .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection(List.of(protocol).iterator()));
    }

    private static ByteBuffer serialize(final Message requestData, final short apiVersion) {
        final var cache = new ObjectSerializationCache();
        final var buffer = ByteBuffer.allocate(requestData.size(cache, apiVersion));
        requestData.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();
        return buffer;
    }

    private static JoinGroupResponseData parseJoinGroupResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.JOIN_GROUP, apiVersion);
        final var data = new JoinGroupResponseData();
        data.read(new ByteBufferAccessor(buffer), apiVersion);
        return data;
    }

    private static SyncGroupResponseData parseSyncGroupResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.SYNC_GROUP, apiVersion);
        final var data = new SyncGroupResponseData();
        data.read(new ByteBufferAccessor(buffer), apiVersion);
        return data;
    }

    private static HeartbeatResponseData parseHeartbeatResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.HEARTBEAT, apiVersion);
        final var data = new HeartbeatResponseData();
        data.read(new ByteBufferAccessor(buffer), apiVersion);
        return data;
    }

    private static LeaveGroupResponseData parseLeaveGroupResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.LEAVE_GROUP, apiVersion);
        final var data = new LeaveGroupResponseData();
        data.read(new ByteBufferAccessor(buffer), apiVersion);
        return data;
    }

    private static void skipResponseHeader(final ByteBuffer buffer, final ApiKeys apiKey, final short apiVersion) {
        final var headerVersion = apiKey.responseHeaderVersion(apiVersion);
        final var headerBytes = headerVersion >= 1 ? 5 : 4;
        buffer.position(buffer.position() + headerBytes);
    }
}
