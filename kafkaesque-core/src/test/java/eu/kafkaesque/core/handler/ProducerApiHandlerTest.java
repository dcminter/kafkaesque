package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.EventStore;
import eu.kafkaesque.core.storage.TopicStore;
import eu.kafkaesque.core.storage.TransactionState;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ProducerApiHandler}.
 */
class ProducerApiHandlerTest {

    private EventStore eventStore;
    private ProducerApiHandler handler;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
        handler = new ProducerApiHandler(eventStore);
    }

    @Test
    void generateProduceResponse_shouldStoreRecordAndReturnSuccessCode() {
        final var topic = "my-topic";
        final var partition = 0;

        final var response = invokeProduceRequest(topic, partition, null, "key-1", "value-1");

        assertThat(response).isNotNull();
        final var responseData = parseProduceResponse(response);
        assertThat(responseData.responses()).hasSize(1);
        final var topicResponse = responseData.responses().iterator().next();
        assertThat(topicResponse.name()).isEqualTo(topic);
        assertThat(topicResponse.partitionResponses()).hasSize(1);
        assertThat(topicResponse.partitionResponses().get(0).errorCode()).isZero();

        assertThat(eventStore.getRecordCount(topic, partition)).isEqualTo(1L);
        assertThat(eventStore.getRecords(topic, partition).get(0).key()).isEqualTo("key-1");
        assertThat(eventStore.getRecords(topic, partition).get(0).value()).isEqualTo("value-1");
    }

    @Test
    void generateProduceResponse_shouldReturnBaseOffsetZeroForFirstRecord() {
        final var response = invokeProduceRequest("topic", 0, null, "k", "v");

        final var responseData = parseProduceResponse(response);
        final var partitionResponse = responseData.responses().iterator().next().partitionResponses().get(0);
        assertThat(partitionResponse.baseOffset()).isZero();
    }

    @Test
    void generateProduceResponse_withAutoCreateDisabled_andUnknownTopic_shouldReturnError() {
        final var topicStore = new TopicStore();
        final var restrictedHandler = new ProducerApiHandler(eventStore, topicStore, false);

        final var response = invokeProduceRequest(restrictedHandler, "unknown-topic", 0, null, "k", "v");

        final var responseData = parseProduceResponse(response);
        final var partitionResponse = responseData.responses().iterator().next().partitionResponses().get(0);
        assertThat(partitionResponse.errorCode()).isNotZero();
    }

    @Test
    void generateProduceResponse_transactional_shouldStorePendingRecord() {
        final var topic = "txn-topic";
        final var txnId = "my-txn";

        invokeProduceRequest(topic, 0, txnId, "k", "v");

        assertThat(eventStore.getTransactionState(topic, 0, 0L)).isEqualTo(TransactionState.PENDING);
        assertThat(eventStore.getRecords(topic, 0, (byte) 1)).isEmpty();
        assertThat(eventStore.getRecords(topic, 0, (byte) 0)).hasSize(1);
    }

    @Test
    void generateProduceResponse_withMalformedBuffer_shouldReturnNull() {
        final var header = new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion(), "test", 1);
        final var emptyBuffer = ByteBuffer.allocate(0);

        final var response = handler.generateProduceResponse(header, emptyBuffer);

        assertThat(response).isNull();
    }

    // --- helpers ---

    private ByteBuffer invokeProduceRequest(
            final String topic, final int partition,
            final String transactionalId, final String key, final String value) {
        return invokeProduceRequest(handler, topic, partition, transactionalId, key, value);
    }

    private static ByteBuffer invokeProduceRequest(
            final ProducerApiHandler producerHandler,
            final String topic, final int partition,
            final String transactionalId, final String key, final String value) {
        final var apiVersion = ApiKeys.PRODUCE.latestVersion();

        final var records = buildMemoryRecords(key, value);

        final var partitionData = new ProduceRequestData.PartitionProduceData()
            .setIndex(partition)
            .setRecords(records);

        final var topicData = new ProduceRequestData.TopicProduceData()
            .setName(topic)
            .setPartitionData(java.util.List.of(partitionData));

        final var requestData = new ProduceRequestData()
            .setAcks((short) 1)
            .setTimeoutMs(5000)
            .setTransactionalId(transactionalId)
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(java.util.List.of(topicData).iterator()));

        final var cache = new ObjectSerializationCache();
        final var buffer = ByteBuffer.allocate(requestData.size(cache, apiVersion));
        requestData.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();

        final var header = new RequestHeader(ApiKeys.PRODUCE, apiVersion, "test-client", 1);
        return producerHandler.generateProduceResponse(header, buffer);
    }

    private static MemoryRecords buildMemoryRecords(final String key, final String value) {
        final var builder = MemoryRecords.builder(
            ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(System.currentTimeMillis(),
            key == null ? null : key.getBytes(java.nio.charset.StandardCharsets.UTF_8),
            value == null ? null : value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return builder.build();
    }

    private static ProduceResponseData parseProduceResponse(final ByteBuffer buffer) {
        final var apiVersion = ApiKeys.PRODUCE.latestVersion();
        final var headerVersion = ApiKeys.PRODUCE.responseHeaderVersion(apiVersion);
        final var headerBytes = headerVersion >= 1 ? 5 : 4;
        buffer.position(buffer.position() + headerBytes);
        final var responseData = new ProduceResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }
}
