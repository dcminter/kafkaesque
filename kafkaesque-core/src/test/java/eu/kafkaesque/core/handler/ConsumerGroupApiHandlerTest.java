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
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ConsumerGroupApiHandler}.
 */
class ConsumerGroupApiHandlerTest {

    private GroupCoordinator groupCoordinator;
    private ConsumerGroupApiHandler handler;
    private final List<DeferredResponse> dispatched = new ArrayList<>();

    @BeforeEach
    void setUp() {
        groupCoordinator = new GroupCoordinator();
        handler = new ConsumerGroupApiHandler(groupCoordinator, dispatched::add);
    }

    @Test
    void generateJoinGroupResponse_shouldDeferResponseAndDispatchViaCallback() {
        final var apiVersion = ApiKeys.JOIN_GROUP.latestVersion();
        final var requestData = buildJoinGroupRequest("test-group", "");
        final var header = new RequestHeader(ApiKeys.JOIN_GROUP, apiVersion, "test-client", 1);

        final var immediate = handler.generateJoinGroupResponse(
            header, serialize(requestData, apiVersion), null, validKey());

        assertThat(immediate).isNull();

        waitFor(() -> !dispatched.isEmpty(),
            ConsumerGroupApiHandler.REBALANCE_WINDOW_MS * 2, TimeUnit.MILLISECONDS);

        assertThat(dispatched).hasSize(1);
        final var responseData = parseJoinGroupResponse(dispatched.get(0).response(), apiVersion);
        assertThat(responseData.errorCode()).isZero();
        assertThat(responseData.memberId()).isNotBlank();
        assertThat(responseData.leader()).isEqualTo(responseData.memberId());
    }

    @Test
    void generateJoinGroupResponse_shouldRegisterMemberInCoordinator() {
        final var apiVersion = ApiKeys.JOIN_GROUP.latestVersion();
        final var requestData = buildJoinGroupRequest("test-group", "");
        final var header = new RequestHeader(ApiKeys.JOIN_GROUP, apiVersion, "test-client", 1);

        handler.generateJoinGroupResponse(
            header, serialize(requestData, apiVersion), null, validKey());

        assertThat(groupCoordinator.getGenerationId("test-group")).isEqualTo(1);
    }

    @Test
    void generateJoinGroupResponse_twoMembersInSameGroup_shouldRespondWithLeaderAndFollower() {
        final var apiVersion = ApiKeys.JOIN_GROUP.latestVersion();
        final var headerA = new RequestHeader(ApiKeys.JOIN_GROUP, apiVersion, "client-a", 1);
        final var headerB = new RequestHeader(ApiKeys.JOIN_GROUP, apiVersion, "client-b", 2);

        handler.generateJoinGroupResponse(
            headerA,
            serialize(buildJoinGroupRequest("shared-group", ""), apiVersion),
            null,
            validKey());
        handler.generateJoinGroupResponse(
            headerB,
            serialize(buildJoinGroupRequest("shared-group", ""), apiVersion),
            null,
            validKey());

        waitFor(() -> dispatched.size() >= 2,
            ConsumerGroupApiHandler.REBALANCE_WINDOW_MS * 2, TimeUnit.MILLISECONDS);

        assertThat(dispatched).hasSize(2);

        final var responses = dispatched.stream()
            .map(d -> parseJoinGroupResponse(d.response(), apiVersion))
            .toList();

        // Both should share the same leader and generation
        assertThat(responses.get(0).leader()).isEqualTo(responses.get(1).leader());
        assertThat(responses.get(0).generationId()).isEqualTo(responses.get(1).generationId());

        // The leader's response includes the full member list; the follower's is empty
        final var leaderResponse = responses.stream()
            .filter(r -> r.memberId().equals(r.leader()))
            .findFirst()
            .orElseThrow();
        assertThat(leaderResponse.members()).hasSize(2);

        final var followerResponse = responses.stream()
            .filter(r -> !r.memberId().equals(r.leader()))
            .findFirst()
            .orElseThrow();
        assertThat(followerResponse.members()).isEmpty();
    }

    @Test
    void generateSyncGroupResponse_shouldReturnSuccessCode() {
        // First join the group
        final var joinApiVersion = ApiKeys.JOIN_GROUP.latestVersion();
        final var joinHeader = new RequestHeader(ApiKeys.JOIN_GROUP, joinApiVersion, "client", 1);
        handler.generateJoinGroupResponse(
            joinHeader,
            serialize(buildJoinGroupRequest("group-1", ""), joinApiVersion),
            null,
            validKey());

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
        final var copy = buffer.duplicate();
        skipResponseHeader(copy, ApiKeys.JOIN_GROUP, apiVersion);
        final var data = new JoinGroupResponseData();
        data.read(new ByteBufferAccessor(copy), apiVersion);
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

    /**
     * Creates a minimal valid-looking {@link SelectionKey} stub for tests that do not need
     * a real NIO channel but must pass a non-null key to the handler.
     *
     * @return a stub key whose {@link SelectionKey#isValid()} returns {@code true}
     */
    @SuppressWarnings("PMD.ReturnEmptyCollectionRatherThanNull")
    private static SelectionKey validKey() {
        return new SelectionKey() {
            @Override public SelectableChannel channel() { return null; }
            @Override public Selector selector() { return null; }
            @Override public boolean isValid() { return true; }
            @Override public void cancel() { }
            @Override public int interestOps() { return 0; }
            @Override public SelectionKey interestOps(final int ops) { return this; }
            @Override public int readyOps() { return 0; }
        };
    }

    /**
     * Polls {@code condition} every 10 ms until it returns {@code true} or {@code timeoutMs} elapses.
     *
     * @param condition  condition to poll
     * @param timeout    maximum wait duration
     * @param unit       time unit for {@code timeout}
     * @throws AssertionError if the condition does not become true within the timeout
     */
    private static void waitFor(final Callable<Boolean> condition, final long timeout, final TimeUnit unit) {
        final long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        while (System.currentTimeMillis() < deadline) {
            try {
                if (Boolean.TRUE.equals(condition.call())) {
                    return;
                }
                Thread.sleep(10);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for condition", e);
            } catch (final Exception e) {
                throw new AssertionError("Condition threw an exception", e);
            }
        }
        throw new AssertionError("Condition not satisfied within timeout");
    }
}
