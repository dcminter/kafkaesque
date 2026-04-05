package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.EventStore;
import eu.kafkaesque.core.storage.TopicStore;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
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
        handler = new ConsumerDataApiHandler(eventStore, groupCoordinator, new TopicStore());
    }

    @Test
    void generateListOffsetsResponse_earliest_shouldReturnOffsetZero() {
        final var apiVersion = ApiKeys.LIST_OFFSETS.latestVersion();
        final var partitionRequest = new ListOffsetsRequestData.ListOffsetsPartition()
            .setPartitionIndex(0)
            .setTimestamp(-2L); // EARLIEST
        final var topicRequest = new ListOffsetsRequestData.ListOffsetsTopic()
            .setName("my-topic")
            .setPartitions(List.of(partitionRequest));
        final var requestData = new ListOffsetsRequestData()
            .setReplicaId(-1)
            .setTopics(List.of(topicRequest));
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
            .setPartitionIndexes(List.of(0));
        final var requestData = new OffsetFetchRequestData()
            .setGroupId("my-group")
            .setTopics(List.of(topicRequest));
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
            .setPartitions(List.of(partition));
        final var requestData = new OffsetCommitRequestData()
            .setGroupId("my-group")
            .setTopics(List.of(topic));
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
            .setPartitions(List.of(partitionRequest));
        final var requestData = new FetchRequestData()
            .setReplicaId(-1)
            .setMaxWaitMs(100)
            .setMinBytes(1)
            .setIsolationLevel((byte) 0)
            .setTopics(List.of(topicRequest));
        final var header = new RequestHeader(ApiKeys.FETCH, apiVersion, "test-client", 4);

        final var response = handler.generateFetchResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull().satisfies(buf -> assertThat(buf.remaining()).isPositive());
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
}
