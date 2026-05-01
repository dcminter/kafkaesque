package eu.kafkaesque.core.handler;

import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.AbstractRequest;
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
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;
import static java.util.List.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static java.lang.Thread.sleep;
import static java.lang.Thread.currentThread;

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
            header, parseAs(ApiKeys.JOIN_GROUP, apiVersion, requestData), null, validKey());

        assertThat(immediate).isNull();

        waitFor(() -> !dispatched.isEmpty(),
            ConsumerGroupApiHandler.REBALANCE_WINDOW_MS * 2, MILLISECONDS);

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
            header, parseAs(ApiKeys.JOIN_GROUP, apiVersion, requestData), null, validKey());

        assertThat(groupCoordinator.getGenerationId("test-group")).isEqualTo(1);
    }

    @Test
    void generateJoinGroupResponse_twoMembersInSameGroup_shouldRespondWithLeaderAndFollower() {
        final var apiVersion = ApiKeys.JOIN_GROUP.latestVersion();
        final var headerA = new RequestHeader(ApiKeys.JOIN_GROUP, apiVersion, "client-a", 1);
        final var headerB = new RequestHeader(ApiKeys.JOIN_GROUP, apiVersion, "client-b", 2);

        handler.generateJoinGroupResponse(
            headerA,
            parseAs(ApiKeys.JOIN_GROUP, apiVersion, buildJoinGroupRequest("shared-group", "")),
            null,
            validKey());
        handler.generateJoinGroupResponse(
            headerB,
            parseAs(ApiKeys.JOIN_GROUP, apiVersion, buildJoinGroupRequest("shared-group", "")),
            null,
            validKey());

        waitFor(() -> dispatched.size() >= 2,
            ConsumerGroupApiHandler.REBALANCE_WINDOW_MS * 2, MILLISECONDS);

        assertThat(dispatched).hasSize(2);

        final var responses = dispatched.stream()
            .map(d -> parseJoinGroupResponse(d.response(), apiVersion))
            .collect(Collectors.toList());

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
            parseAs(ApiKeys.JOIN_GROUP, joinApiVersion, buildJoinGroupRequest("group-1", "")),
            null,
            validKey());

        // Then sync
        final var syncApiVersion = ApiKeys.SYNC_GROUP.latestVersion();
        final var syncRequestData = new SyncGroupRequestData()
            .setGroupId("group-1")
            .setGenerationId(1)
            .setMemberId("member-1")
            .setAssignments(of());
        final var syncHeader = new RequestHeader(ApiKeys.SYNC_GROUP, syncApiVersion, "client", 2);

        final var response = handler.generateSyncGroupResponse(
            syncHeader, parseAs(ApiKeys.SYNC_GROUP, syncApiVersion, syncRequestData), null, validKey());

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

        final var response = handler.generateHeartbeatResponse(header,
            parseAs(ApiKeys.HEARTBEAT, apiVersion, requestData));

        assertThat(response).isNotNull();
        final var responseData = parseHeartbeatResponse(response, apiVersion);
        assertThat(responseData.errorCode()).isZero();
    }

    @Test
    void generateLeaveGroupResponse_shouldReturnSuccessCode() {
        final var apiVersion = ApiKeys.LEAVE_GROUP.latestVersion();
        final var requestData = new LeaveGroupRequestData()
            .setGroupId("my-group")
            .setMembers(of(new LeaveGroupRequestData.MemberIdentity().setMemberId("member-1")));
        final var header = new RequestHeader(ApiKeys.LEAVE_GROUP, apiVersion, "test-client", 4);

        final var response = handler.generateLeaveGroupResponse(header,
            parseAs(ApiKeys.LEAVE_GROUP, apiVersion, requestData));

        assertThat(response).isNotNull();
        final var responseData = parseLeaveGroupResponse(response, apiVersion);
        assertThat(responseData.errorCode()).isZero();
    }

    @Test
    void generateListGroupsResponse_shouldReturnRegisteredGroups() {
        groupCoordinator.joinGroup("group-a", "", new byte[0]);
        groupCoordinator.joinGroup("group-b", "", new byte[0]);

        final var apiVersion = ApiKeys.LIST_GROUPS.latestVersion();
        final var requestData = new ListGroupsRequestData();
        final var header = new RequestHeader(ApiKeys.LIST_GROUPS, apiVersion, "test-client", 10);

        final var response = handler.generateListGroupsResponse(header,
            parseAs(ApiKeys.LIST_GROUPS, apiVersion, requestData));

        assertThat(response).isNotNull();
        final var responseData = parseListGroupsResponse(response, apiVersion);
        assertThat(responseData.errorCode()).isZero();
        final var groupIds = responseData.groups().stream()
            .map(ListGroupsResponseData.ListedGroup::groupId)
            .collect(Collectors.toList());
        assertThat(groupIds).containsExactlyInAnyOrder("group-a", "group-b");
    }

    @Test
    void generateConsumerGroupDescribeResponse_shouldReturnMembersForGroup() {
        groupCoordinator.joinGroup("test-group", "", new byte[0]);

        final var apiVersion = ApiKeys.CONSUMER_GROUP_DESCRIBE.latestVersion();
        final var requestData = new ConsumerGroupDescribeRequestData()
            .setGroupIds(of("test-group"));
        final var header = new RequestHeader(
            ApiKeys.CONSUMER_GROUP_DESCRIBE, apiVersion, "test-client", 11);

        final var response = handler.generateConsumerGroupDescribeResponse(
            header, parseAs(ApiKeys.CONSUMER_GROUP_DESCRIBE, apiVersion, requestData));

        assertThat(response).isNotNull();
        final var responseData = parseConsumerGroupDescribeResponse(response, apiVersion);
        assertThat(responseData.groups()).hasSize(1);
        final var group = responseData.groups().get(0);
        assertThat(group.groupId()).isEqualTo("test-group");
        assertThat(group.errorCode()).isZero();
        assertThat(group.members()).hasSize(1);
    }

    @Test
    void generateDeleteGroupsResponse_shouldDeleteGroupAndReturnSuccess() {
        groupCoordinator.joinGroup("doomed-group", "", new byte[0]);
        assertThat(groupCoordinator.getGroupIds()).contains("doomed-group");

        final var apiVersion = ApiKeys.DELETE_GROUPS.latestVersion();
        final var requestData = new DeleteGroupsRequestData()
            .setGroupsNames(of("doomed-group"));
        final var header = new RequestHeader(ApiKeys.DELETE_GROUPS, apiVersion, "test-client", 12);

        final var response = handler.generateDeleteGroupsResponse(header,
            parseAs(ApiKeys.DELETE_GROUPS, apiVersion, requestData));

        assertThat(response).isNotNull();
        final var responseData = parseDeleteGroupsResponse(response, apiVersion);
        final var result = responseData.results().find("doomed-group");
        assertThat(result).isNotNull();
        assertThat(result.errorCode()).isZero();
        assertThat(groupCoordinator.getGroupIds()).doesNotContain("doomed-group");
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
            .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection(of(protocol).iterator()));
    }

    private static ByteBuffer serialize(final Message requestData, final short apiVersion) {
        final var cache = new ObjectSerializationCache();
        final var buffer = ByteBuffer.allocate(requestData.size(cache, apiVersion));
        requestData.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();
        return buffer;
    }

    /**
     * Serialises {@code data} and parses it back as the typed {@link AbstractRequest} the
     * handler now expects, mirroring how {@code KafkaProtocolHandler.dispatchRequest}
     * pre-parses the body before invoking a handler.
     */
    @SuppressWarnings("unchecked")
    private static <T extends AbstractRequest> T parseAs(
            final ApiKeys apiKey, final short apiVersion, final ApiMessage data) {
        return (T) AbstractRequest.parseRequest(apiKey, apiVersion, serialize((Message) data, apiVersion)).request;
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

    private static ListGroupsResponseData parseListGroupsResponse(
            final ByteBuffer buffer, final short apiVersion) {
        final var copy = buffer.duplicate();
        skipResponseHeader(copy, ApiKeys.LIST_GROUPS, apiVersion);
        final var data = new ListGroupsResponseData();
        data.read(new ByteBufferAccessor(copy), apiVersion);
        return data;
    }

    private static ConsumerGroupDescribeResponseData parseConsumerGroupDescribeResponse(
            final ByteBuffer buffer, final short apiVersion) {
        final var copy = buffer.duplicate();
        skipResponseHeader(copy, ApiKeys.CONSUMER_GROUP_DESCRIBE, apiVersion);
        final var data = new ConsumerGroupDescribeResponseData();
        data.read(new ByteBufferAccessor(copy), apiVersion);
        return data;
    }

    private static DeleteGroupsResponseData parseDeleteGroupsResponse(
            final ByteBuffer buffer, final short apiVersion) {
        final var copy = buffer.duplicate();
        skipResponseHeader(copy, ApiKeys.DELETE_GROUPS, apiVersion);
        final var data = new DeleteGroupsResponseData();
        data.read(new ByteBufferAccessor(copy), apiVersion);
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
        final long deadline = currentTimeMillis() + unit.toMillis(timeout);
        while (currentTimeMillis() < deadline) {
            try {
                if (Boolean.TRUE.equals(condition.call())) {
                    return;
                }
                sleep(10);
            } catch (final InterruptedException e) {
                currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for condition", e);
            } catch (final Exception e) {
                throw new AssertionError("Condition threw an exception", e);
            }
        }
        throw new AssertionError("Condition not satisfied within timeout");
    }
}
