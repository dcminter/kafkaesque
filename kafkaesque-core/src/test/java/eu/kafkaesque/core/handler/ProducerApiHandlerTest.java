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
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.util.List.of;
import static org.apache.kafka.common.protocol.Errors.INVALID_PRODUCER_EPOCH;
import static org.apache.kafka.common.protocol.Errors.OUT_OF_ORDER_SEQUENCE_NUMBER;
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

        assertThat(eventStore.getTransactionState(topic, 0, 0L)).hasValue(TransactionState.PENDING);
        assertThat(eventStore.getRecords(topic, 0, (byte) 1)).isEmpty();
        assertThat(eventStore.getRecords(topic, 0, (byte) 0)).hasSize(1);
    }

    @Test
    void generateProduceResponse_duplicateIdempotentBatch_shouldNotStoreAgain() {
        final var topic = "idempotent-topic";
        final var partition = 0;
        final long producerId = 42L;
        final short producerEpoch = 0;
        final int baseSequence = 0;

        // First send — should store the record
        invokeIdempotentProduceRequest(topic, partition, producerId, producerEpoch, baseSequence, "k", "v");
        assertThat(eventStore.getRecordCount(topic, partition)).isEqualTo(1L);

        // Second send with the same (producerId, epoch, baseSequence) — duplicate, must not store again
        final var response = invokeIdempotentProduceRequest(
            topic, partition, producerId, producerEpoch, baseSequence, "k", "v");

        assertThat(eventStore.getRecordCount(topic, partition)).isEqualTo(1L);
        final var responseData = parseProduceResponse(response);
        assertThat(responseData.responses().iterator().next()
            .partitionResponses().get(0).errorCode()).isZero();
    }

    @Test
    void generateProduceResponse_outOfOrderIdempotentSequence_shouldReturnError() {
        final var topic = "idempotent-topic";
        final var partition = 0;
        final long producerId = 99L;
        final short producerEpoch = 0;

        // First batch for this producer must have baseSequence=0; sending 5 is out-of-order
        final var response = invokeIdempotentProduceRequest(
            topic, partition, producerId, producerEpoch, 5, "k", "v");

        assertThat(eventStore.getRecordCount(topic, partition)).isZero();
        final var responseData = parseProduceResponse(response);
        assertThat(responseData.responses().iterator().next()
            .partitionResponses().get(0).errorCode())
            .isEqualTo(OUT_OF_ORDER_SEQUENCE_NUMBER.code());
    }

    @Test
    void generateProduceResponse_duplicateIdempotentBatch_shouldReturnCachedOffset() {
        final var topic = "idempotent-topic";
        final var partition = 0;
        final long producerId = 55L;
        final short producerEpoch = 0;

        // First send — offset 0 is assigned
        final var firstResponse = invokeIdempotentProduceRequest(
            topic, partition, producerId, producerEpoch, 0, "k", "v");
        final var firstOffset = parseProduceResponse(firstResponse)
            .responses().iterator().next().partitionResponses().get(0).baseOffset();

        // Duplicate — must return the original cached offset, not a new one
        final var duplicateResponse = invokeIdempotentProduceRequest(
            topic, partition, producerId, producerEpoch, 0, "k", "v");

        assertThat(eventStore.getRecordCount(topic, partition)).isEqualTo(1L);
        final var duplicateOffset = parseProduceResponse(duplicateResponse)
            .responses().iterator().next().partitionResponses().get(0).baseOffset();
        assertThat(duplicateOffset)
            .as("duplicate response must return the original cached offset")
            .isEqualTo(firstOffset);
    }

    @Test
    void generateProduceResponse_sequentialIdempotentBatches_shouldStoreAll() {
        final var topic = "idempotent-topic";
        final var partition = 0;
        final long producerId = 77L;
        final short producerEpoch = 0;

        invokeIdempotentProduceRequest(topic, partition, producerId, producerEpoch, 0, "k0", "v0");
        invokeIdempotentProduceRequest(topic, partition, producerId, producerEpoch, 1, "k1", "v1");
        final var response = invokeIdempotentProduceRequest(
            topic, partition, producerId, producerEpoch, 2, "k2", "v2");

        assertThat(eventStore.getRecordCount(topic, partition)).isEqualTo(3L);
        assertThat(parseProduceResponse(response)
            .responses().iterator().next()
            .partitionResponses().get(0).errorCode()).isZero();
    }

    @Test
    void generateProduceResponse_staleEpoch_shouldReturnInvalidProducerEpochError() {
        final var topic = "idempotent-topic";
        final var partition = 0;
        final long producerId = 88L;

        // Establish epoch 0, then advance to epoch 1
        invokeIdempotentProduceRequest(topic, partition, producerId, (short) 0, 0, "k", "v");
        invokeIdempotentProduceRequest(topic, partition, producerId, (short) 1, 0, "k", "v");

        // A retry from the old epoch-0 producer must be fenced
        final var response = invokeIdempotentProduceRequest(
            topic, partition, producerId, (short) 0, 1, "k", "v");

        assertThat(parseProduceResponse(response)
            .responses().iterator().next()
            .partitionResponses().get(0).errorCode())
            .isEqualTo(INVALID_PRODUCER_EPOCH.code());
    }

    // --- helpers ---

    private ByteBuffer invokeProduceRequest(
            final String topic, final int partition,
            final String transactionalId, final String key, final String value) {
        return invokeProduceRequest(handler, topic, partition, transactionalId, key, value);
    }

    private ByteBuffer invokeIdempotentProduceRequest(
            final String topic, final int partition,
            final long producerId, final short producerEpoch, final int baseSequence,
            final String key, final String value) {
        return invokeProduceRequest(
            handler, topic, partition, null,
            buildIdempotentMemoryRecords(producerId, producerEpoch, baseSequence, key, value));
    }

    private static ByteBuffer invokeProduceRequest(
            final ProducerApiHandler producerHandler,
            final String topic, final int partition,
            final String transactionalId, final String key, final String value) {
        return invokeProduceRequest(
            producerHandler, topic, partition, transactionalId, buildMemoryRecords(key, value));
    }

    private static ByteBuffer invokeProduceRequest(
            final ProducerApiHandler producerHandler,
            final String topic, final int partition,
            final String transactionalId, final MemoryRecords records) {
        final var apiVersion = ApiKeys.PRODUCE.latestVersion();

        final var partitionData = new ProduceRequestData.PartitionProduceData()
            .setIndex(partition)
            .setRecords(records);

        final var topicData = new ProduceRequestData.TopicProduceData()
            .setName(topic)
            .setPartitionData(of(partitionData));

        final var requestData = new ProduceRequestData()
            .setAcks((short) 1)
            .setTimeoutMs(5000)
            .setTransactionalId(transactionalId)
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(of(topicData).iterator()));

        final var cache = new ObjectSerializationCache();
        final var buffer = ByteBuffer.allocate(requestData.size(cache, apiVersion));
        requestData.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();

        final var produceRequest = (ProduceRequest) AbstractRequest.parseRequest(
            ApiKeys.PRODUCE, apiVersion, buffer).request;
        final var header = new RequestHeader(ApiKeys.PRODUCE, apiVersion, "test-client", 1);
        return producerHandler.generateProduceResponse(header, produceRequest);
    }

    private static MemoryRecords buildIdempotentMemoryRecords(
            final long producerId, final short producerEpoch, final int baseSequence,
            final String key, final String value) {
        final var builder = MemoryRecords.idempotentBuilder(
            ByteBuffer.allocate(1024), Compression.NONE, 0L, producerId, producerEpoch, baseSequence);
        builder.append(System.currentTimeMillis(),
            key == null ? null : key.getBytes(StandardCharsets.UTF_8),
            value == null ? null : value.getBytes(StandardCharsets.UTF_8));
        return builder.build();
    }

    private static MemoryRecords buildMemoryRecords(final String key, final String value) {
        final var builder = MemoryRecords.builder(
            ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(System.currentTimeMillis(),
            key == null ? null : key.getBytes(StandardCharsets.UTF_8),
            value == null ? null : value.getBytes(StandardCharsets.UTF_8));
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
