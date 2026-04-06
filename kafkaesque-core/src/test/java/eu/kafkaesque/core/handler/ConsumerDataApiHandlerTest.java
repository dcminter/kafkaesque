package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.EventStore;
import eu.kafkaesque.core.storage.TopicStore;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ConsumerDataApiHandler}.
 */
class ConsumerDataApiHandlerTest {

    private EventStore eventStore;
    private GroupCoordinator groupCoordinator;
    private ConsumerDataApiHandler handler;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
        groupCoordinator = new GroupCoordinator();
        handler = new ConsumerDataApiHandler(
            eventStore, groupCoordinator, new TopicStore(), new FetchSessionCoordinator());
    }

    @Test
    void generateListOffsetsResponse_earliest_shouldReturnOffsetZero() {
        final var apiVersion = ApiKeys.LIST_OFFSETS.latestVersion();
        final var partitionRequest = new ListOffsetsRequestData.ListOffsetsPartition()
            .setPartitionIndex(0)
            .setTimestamp(-2L); // EARLIEST
        final var topicRequest = new ListOffsetsRequestData.ListOffsetsTopic()
            .setName("my-topic")
            .setPartitions(of(partitionRequest));
        final var requestData = new ListOffsetsRequestData()
            .setReplicaId(-1)
            .setTopics(of(topicRequest));
        final var header = new RequestHeader(ApiKeys.LIST_OFFSETS, apiVersion, "test-client", 1);

        final var response = handler.generateListOffsetsResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseListOffsetsResponse(response, apiVersion);
        final var partition = responseData.topics().get(0).partitions().get(0);
        assertThat(partition.errorCode()).isZero();
        assertThat(partition.offset()).isZero();
    }

    @Test
    void generateOffsetFetchResponse_withNoCommit_shouldReturnMinusOne() {
        final var apiVersion = (short) 7; // Use version < 8 for simpler group-level request
        final var topicRequest = new OffsetFetchRequestData.OffsetFetchRequestTopic()
            .setName("my-topic")
            .setPartitionIndexes(of(0));
        final var requestData = new OffsetFetchRequestData()
            .setGroupId("my-group")
            .setTopics(of(topicRequest));
        final var header = new RequestHeader(ApiKeys.OFFSET_FETCH, apiVersion, "test-client", 2);

        final var response = handler.generateOffsetFetchResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseOffsetFetchResponse(response, apiVersion);
        assertThat(responseData.topics().get(0).partitions().get(0).committedOffset()).isEqualTo(-1L);
    }

    @Test
    void generateOffsetCommitResponse_shouldPersistOffsetAndReturnSuccess() {
        final var apiVersion = ApiKeys.OFFSET_COMMIT.latestVersion();
        final var partition = new OffsetCommitRequestData.OffsetCommitRequestPartition()
            .setPartitionIndex(0)
            .setCommittedOffset(42L);
        final var topic = new OffsetCommitRequestData.OffsetCommitRequestTopic()
            .setName("my-topic")
            .setPartitions(of(partition));
        final var requestData = new OffsetCommitRequestData()
            .setGroupId("my-group")
            .setTopics(of(topic));
        final var header = new RequestHeader(ApiKeys.OFFSET_COMMIT, apiVersion, "test-client", 3);

        final var response = handler.generateOffsetCommitResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseOffsetCommitResponse(response, apiVersion);
        assertThat(responseData.topics().get(0).partitions().get(0).errorCode()).isZero();

        // Verify the offset was actually committed in the coordinator
        assertThat(groupCoordinator.getCommittedOffset("my-group", "my-topic", 0)).isEqualTo(42L);
    }

    @Test
    void generateFetchResponse_withStoredRecords_shouldReturnRecords() {
        // Store a record first
        final var timestamp = System.currentTimeMillis();
        eventStore.storeRecord("my-topic", 0, timestamp, "key-1", "value-1");

        final var apiVersion = (short) 12; // Capped at v12 per handler constants
        final var partitionRequest = new FetchRequestData.FetchPartition()
            .setPartition(0)
            .setFetchOffset(0L)
            .setPartitionMaxBytes(1024 * 1024);
        final var topicRequest = new FetchRequestData.FetchTopic()
            .setTopic("my-topic")
            .setPartitions(of(partitionRequest));
        final var requestData = new FetchRequestData()
            .setReplicaId(-1)
            .setMaxWaitMs(100)
            .setMinBytes(1)
            .setIsolationLevel((byte) 0)
            .setTopics(of(topicRequest));
        final var header = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 4);

        final var response = handler.generateFetchResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull().satisfies(buf -> assertThat(buf.remaining()).isPositive());
    }

    @Test
    void generateFetchResponse_initialEpoch_shouldCreateSessionAndReturnNonZeroSessionId() {
        eventStore.storeRecord("my-topic", 0, System.currentTimeMillis(), "k", "v");

        final var apiVersion = (short) 12;
        final var requestData = buildFetchRequest("my-topic", 0, 0L, 0, 0);
        final var header = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 5);

        final var response = handler.generateFetchResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseFetchResponse(response, apiVersion);
        assertThat(responseData.sessionId()).isPositive();
        assertThat(responseData.errorCode()).isZero();
    }

    @Test
    void generateFetchResponse_legacyEpoch_shouldReturnSessionIdZero() {
        eventStore.storeRecord("my-topic", 0, System.currentTimeMillis(), "k", "v");

        final var apiVersion = (short) 12;
        // Default sessionEpoch is -1 (FINAL_EPOCH / legacy mode)
        final var requestData = buildFetchRequest("my-topic", 0, 0L, 0, -1);
        final var header = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 6);

        final var response = handler.generateFetchResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseFetchResponse(response, apiVersion);
        assertThat(responseData.sessionId()).isZero();
        assertThat(responseData.errorCode()).isZero();
    }

    @Test
    void generateFetchResponse_incrementalFetch_shouldReturnOnlyPartitionsWithNewData() {
        // Set up: create a session with a full fetch first
        final var apiVersion = (short) 12;
        final var fullFetchData = buildFetchRequest("my-topic", 0, 0L, 0, 0);
        final var fullFetchHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 7);
        final var fullFetchResponse = handler.generateFetchResponse(
            fullFetchHeader, serialize(fullFetchData, apiVersion));
        final int sessionId = parseFetchResponse(fullFetchResponse, apiVersion).sessionId();

        // Now store a record so the incremental fetch has something to return
        eventStore.storeRecord("my-topic", 0, System.currentTimeMillis(), "k", "v");

        // Incremental fetch: epoch=1, only send changed partition (offset advanced to 0)
        final var incFetchData = buildFetchRequest("my-topic", 0, 0L, sessionId, 1);
        final var incFetchHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 8);
        final var incFetchResponse = handler.generateFetchResponse(
            incFetchHeader, serialize(incFetchData, apiVersion));

        assertThat(incFetchResponse).isNotNull();
        final var responseData = parseFetchResponse(incFetchResponse, apiVersion);
        assertThat(responseData.errorCode()).isZero();
        assertThat(responseData.sessionId()).isEqualTo(sessionId);
        assertThat(responseData.responses()).isNotEmpty();
    }

    @Test
    void generateFetchResponse_incrementalFetchNoNewData_shouldReturnEmptyResponses() {
        // Full fetch first to create a session
        final var apiVersion = (short) 12;
        final var fullFetchData = buildFetchRequest("my-topic", 0, 0L, 0, 0);
        final var fullFetchHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 9);
        final var fullFetchResponse = handler.generateFetchResponse(
            fullFetchHeader, serialize(fullFetchData, apiVersion));
        final int sessionId = parseFetchResponse(fullFetchResponse, apiVersion).sessionId();

        // Incremental fetch with no new records: expect empty responses list
        final var incFetchData = buildFetchRequest("my-topic", 0, 0L, sessionId, 1);
        final var incFetchHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 10);
        final var incFetchResponse = handler.generateFetchResponse(
            incFetchHeader, serialize(incFetchData, apiVersion));

        assertThat(incFetchResponse).isNotNull();
        final var responseData = parseFetchResponse(incFetchResponse, apiVersion);
        assertThat(responseData.errorCode()).isZero();
        assertThat(responseData.responses()).isEmpty();
    }

    @Test
    void generateFetchResponse_unknownSession_shouldReturnFetchSessionIdNotFound() {
        final var apiVersion = (short) 12;
        final int unknownSessionId = 99999;
        final var requestData = buildFetchRequest("my-topic", 0, 0L, unknownSessionId, 1);
        final var header = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 11);

        final var response = handler.generateFetchResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseFetchResponse(response, apiVersion);
        assertThat(responseData.errorCode())
            .isEqualTo(Errors.FETCH_SESSION_ID_NOT_FOUND.code());
    }

    @Test
    void generateFetchResponse_invalidEpoch_shouldReturnInvalidFetchSessionEpoch() {
        // Create a session first
        final var apiVersion = (short) 12;
        final var fullFetchData = buildFetchRequest("my-topic", 0, 0L, 0, 0);
        final var fullFetchHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 12);
        final var fullFetchResponse = handler.generateFetchResponse(
            fullFetchHeader, serialize(fullFetchData, apiVersion));
        final int sessionId = parseFetchResponse(fullFetchResponse, apiVersion).sessionId();

        // Send wrong epoch (should be 1, but sending 5)
        final var badEpochData = buildFetchRequest("my-topic", 0, 0L, sessionId, 5);
        final var badEpochHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 13);

        final var response = handler.generateFetchResponse(
            badEpochHeader, serialize(badEpochData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseFetchResponse(response, apiVersion);
        assertThat(responseData.errorCode())
            .isEqualTo(Errors.INVALID_FETCH_SESSION_EPOCH.code());
    }

    @Test
    void generateFetchResponse_sessionClose_shouldReturnSessionIdZero() {
        // Create a session first
        final var apiVersion = (short) 12;
        final var fullFetchData = buildFetchRequest("my-topic", 0, 0L, 0, 0);
        final var fullFetchHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 14);
        final var fullFetchResponse = handler.generateFetchResponse(
            fullFetchHeader, serialize(fullFetchData, apiVersion));
        final int sessionId = parseFetchResponse(fullFetchResponse, apiVersion).sessionId();
        assertThat(sessionId).isPositive();

        // Close the session with epoch=-1
        final var closeData = buildFetchRequest("my-topic", 0, 0L, sessionId, -1);
        final var closeHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 15);

        final var response = handler.generateFetchResponse(closeHeader, serialize(closeData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseFetchResponse(response, apiVersion);
        assertThat(responseData.sessionId()).isZero();
        assertThat(responseData.errorCode()).isZero();
    }

    @Test
    void generateFetchResponse_afterSessionClose_incrementalFetchReturnsError() {
        final var apiVersion = (short) 12;
        final var fullFetchData = buildFetchRequest("my-topic", 0, 0L, 0, 0);
        final var fullFetchHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 16);
        final var fullFetchResponse = handler.generateFetchResponse(
            fullFetchHeader, serialize(fullFetchData, apiVersion));
        final int sessionId = parseFetchResponse(fullFetchResponse, apiVersion).sessionId();

        final var closeData = buildFetchRequest("my-topic", 0, 0L, sessionId, -1);
        final var closeHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 17);
        handler.generateFetchResponse(closeHeader, serialize(closeData, apiVersion));

        final var incFetchData = buildFetchRequest("my-topic", 0, 0L, sessionId, 1);
        final var incFetchHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 18);
        final var response = handler.generateFetchResponse(incFetchHeader, serialize(incFetchData, apiVersion));

        assertThat(response).isNotNull();
        assertThat(parseFetchResponse(response, apiVersion).errorCode())
            .isEqualTo(Errors.FETCH_SESSION_ID_NOT_FOUND.code());
    }

    @Test
    void generateFetchResponse_multipleIncrementalFetches_epochAdvancesCorrectly() {
        final var apiVersion = (short) 12;
        final var fullFetchData = buildFetchRequest("my-topic", 0, 0L, 0, 0);
        final var fullFetchHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 19);
        final var fullFetchResponse = handler.generateFetchResponse(
            fullFetchHeader, serialize(fullFetchData, apiVersion));
        final int sessionId = parseFetchResponse(fullFetchResponse, apiVersion).sessionId();

        final var inc1Data = buildFetchRequest("my-topic", 0, 0L, sessionId, 1);
        final var inc1Header = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 20);
        assertThat(parseFetchResponse(
            handler.generateFetchResponse(inc1Header, serialize(inc1Data, apiVersion)), apiVersion)
            .errorCode()).isZero();

        final var inc2Data = buildFetchRequest("my-topic", 0, 0L, sessionId, 2);
        final var inc2Header = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 21);
        assertThat(parseFetchResponse(
            handler.generateFetchResponse(inc2Header, serialize(inc2Data, apiVersion)), apiVersion)
            .errorCode()).isZero();

        // Sending stale epoch should now fail
        final var staleData = buildFetchRequest("my-topic", 0, 0L, sessionId, 2);
        final var staleHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 22);
        assertThat(parseFetchResponse(
            handler.generateFetchResponse(staleHeader, serialize(staleData, apiVersion)), apiVersion)
            .errorCode()).isEqualTo(Errors.INVALID_FETCH_SESSION_EPOCH.code());
    }

    @Test
    void generateFetchResponse_sessionCloseWithStoredData_shouldReturnEmptyResponses() {
        // Given – store data so a naive close response would include it
        eventStore.storeRecord("my-topic", 0, System.currentTimeMillis(), "key-1", "value-1");

        final var apiVersion = (short) 12;
        final var fullFetchHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 30);
        final int sessionId = parseFetchResponse(
            handler.generateFetchResponse(
                fullFetchHeader, serialize(buildFetchRequest("my-topic", 0, 0L, 0, 0), apiVersion)),
            apiVersion).sessionId();
        assertThat(sessionId).isPositive();

        // When – close the session with the topic still present in the request
        final var closeHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 31);
        final var responseData = parseFetchResponse(
            handler.generateFetchResponse(
                closeHeader, serialize(buildFetchRequest("my-topic", 0, 0L, sessionId, -1), apiVersion)),
            apiVersion);

        // Then – per KIP-227 responses must be empty; sessionId must be 0
        assertThat(responseData.errorCode()).isZero();
        assertThat(responseData.sessionId()).isZero();
        assertThat(responseData.responses())
            .as("session-close response must have empty responses per KIP-227")
            .isEmpty();
    }

    @Test
    void generateFetchResponse_sessionCloseForUnknownSession_shouldReturnSessionNotFoundError() {
        // Given – session 99999 was never created
        final var apiVersion = (short) 12;
        final var closeHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 32);

        // When – attempt to close it (epoch=-1 signals close)
        final var responseData = parseFetchResponse(
            handler.generateFetchResponse(
                closeHeader, serialize(buildFetchRequest("my-topic", 0, 0L, 99999, -1), apiVersion)),
            apiVersion);

        // Then – per KIP-227 must return FETCH_SESSION_ID_NOT_FOUND
        assertThat(responseData.errorCode())
            .as("closing a non-existent session must return FETCH_SESSION_ID_NOT_FOUND")
            .isEqualTo(Errors.FETCH_SESSION_ID_NOT_FOUND.code());
    }

    @Test
    void generateFetchResponse_incrementalFetch_forgottenPartition_shouldNotAppearInResponse() {
        final var apiVersion = (short) 12;

        // Given – session tracking two partitions
        final var fullFetchHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 33);
        final int sessionId = parseFetchResponse(
            handler.generateFetchResponse(
                fullFetchHeader,
                serialize(buildTwoPartitionFetchRequest("my-topic", 0, 0), apiVersion)),
            apiVersion).sessionId();
        assertThat(sessionId).isPositive();

        eventStore.storeRecord("my-topic", 0, System.currentTimeMillis(), "key-p0", "value-p0");
        eventStore.storeRecord("my-topic", 1, System.currentTimeMillis(), "key-p1", "value-p1");

        // When – incremental fetch that forgets partition 0
        final var forgotten = new FetchRequestData.ForgottenTopic()
            .setTopic("my-topic").setPartitions(of(0));
        final var incFetchHeader = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 34);
        final var responseData = parseFetchResponse(
            handler.generateFetchResponse(
                incFetchHeader,
                serialize(buildFetchRequestWithForgottenTopics(
                    "my-topic", 1, 0L, sessionId, 1, of(forgotten)), apiVersion)),
            apiVersion);

        // Then – partition 1 present; forgotten partition 0 absent
        assertThat(responseData.errorCode()).isZero();
        final var partitionIndices = responseData.responses().stream()
            .filter(r -> r.topic().equals("my-topic"))
            .findFirst().orElseThrow()
            .partitions().stream()
            .map(FetchResponseData.PartitionData::partitionIndex)
            .toList();
        assertThat(partitionIndices).contains(1);
        assertThat(partitionIndices)
            .as("forgotten partition 0 must not appear in the incremental response")
            .doesNotContain(0);
    }

    // --- helpers ---

    private static ByteBuffer serialize(final Message requestData, final short apiVersion) {
        final var cache = new ObjectSerializationCache();
        final var buffer = ByteBuffer.allocate(requestData.size(cache, apiVersion));
        requestData.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();
        return buffer;
    }

    private static ListOffsetsResponseData parseListOffsetsResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.LIST_OFFSETS, apiVersion);
        final var data = new ListOffsetsResponseData();
        data.read(new ByteBufferAccessor(buffer), apiVersion);
        return data;
    }

    private static OffsetFetchResponseData parseOffsetFetchResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.OFFSET_FETCH, apiVersion);
        final var data = new OffsetFetchResponseData();
        data.read(new ByteBufferAccessor(buffer), apiVersion);
        return data;
    }

    private static OffsetCommitResponseData parseOffsetCommitResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.OFFSET_COMMIT, apiVersion);
        final var data = new OffsetCommitResponseData();
        data.read(new ByteBufferAccessor(buffer), apiVersion);
        return data;
    }

    private static void skipResponseHeader(final ByteBuffer buffer, final ApiKeys apiKey, final short apiVersion) {
        final var headerVersion = apiKey.responseHeaderVersion(apiVersion);
        final var headerBytes = headerVersion >= 1 ? 5 : 4;
        buffer.position(buffer.position() + headerBytes);
    }

    private static FetchRequestData buildFetchRequest(
            final String topic,
            final int partition,
            final long fetchOffset,
            final int sessionId,
            final int sessionEpoch) {
        final var partitionRequest = new FetchRequestData.FetchPartition()
            .setPartition(partition)
            .setFetchOffset(fetchOffset)
            .setPartitionMaxBytes(1024 * 1024);
        final var topicRequest = new FetchRequestData.FetchTopic()
            .setTopic(topic)
            .setPartitions(of(partitionRequest));
        return new FetchRequestData()
            .setReplicaId(-1)
            .setMaxWaitMs(100)
            .setMinBytes(1)
            .setIsolationLevel((byte) 0)
            .setSessionId(sessionId)
            .setSessionEpoch(sessionEpoch)
            .setTopics(of(topicRequest));
    }

    private static FetchResponseData parseFetchResponse(final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.FETCH, apiVersion);
        final var data = new FetchResponseData();
        data.read(new ByteBufferAccessor(buffer), apiVersion);
        return data;
    }

    private static FetchRequestData buildTwoPartitionFetchRequest(
            final String topic, final int sessionId, final int sessionEpoch) {
        final var partition0 = new FetchRequestData.FetchPartition()
            .setPartition(0).setFetchOffset(0L).setPartitionMaxBytes(1024 * 1024);
        final var partition1 = new FetchRequestData.FetchPartition()
            .setPartition(1).setFetchOffset(0L).setPartitionMaxBytes(1024 * 1024);
        final var topicRequest = new FetchRequestData.FetchTopic()
            .setTopic(topic).setPartitions(of(partition0, partition1));
        return new FetchRequestData()
            .setReplicaId(-1).setMaxWaitMs(100).setMinBytes(1)
            .setIsolationLevel((byte) 0)
            .setSessionId(sessionId).setSessionEpoch(sessionEpoch)
            .setTopics(of(topicRequest));
    }

    private static FetchRequestData buildFetchRequestWithForgottenTopics(
            final String topic, final int partition, final long fetchOffset,
            final int sessionId, final int sessionEpoch,
            final List<FetchRequestData.ForgottenTopic> forgottenTopics) {
        final var partitionRequest = new FetchRequestData.FetchPartition()
            .setPartition(partition).setFetchOffset(fetchOffset).setPartitionMaxBytes(1024 * 1024);
        final var topicRequest = new FetchRequestData.FetchTopic()
            .setTopic(topic).setPartitions(of(partitionRequest));
        return new FetchRequestData()
            .setReplicaId(-1).setMaxWaitMs(100).setMinBytes(1)
            .setIsolationLevel((byte) 0)
            .setSessionId(sessionId).setSessionEpoch(sessionEpoch)
            .setTopics(of(topicRequest))
            .setForgottenTopicsData(forgottenTopics);
    }
}
