package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.EventStore;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TransactionApiHandler}.
 */
class TransactionApiHandlerTest {

    private EventStore eventStore;
    private TransactionCoordinator transactionCoordinator;
    private GroupCoordinator groupCoordinator;
    private TransactionApiHandler handler;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
        transactionCoordinator = new TransactionCoordinator(eventStore);
        groupCoordinator = new GroupCoordinator();
        handler = new TransactionApiHandler(transactionCoordinator, groupCoordinator);
    }

    @Test
    void generateInitProducerIdResponse_shouldReturnNonNullBuffer() {
        final var apiVersion = ApiKeys.INIT_PRODUCER_ID.latestVersion();
        final var requestData = new InitProducerIdRequestData()
            .setTransactionalId("my-txn")
            .setTransactionTimeoutMs(60000);
        final var header = new RequestHeader(ApiKeys.INIT_PRODUCER_ID, apiVersion, "test-client", 1);

        final var response = handler.generateInitProducerIdResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull().satisfies(buf -> assertThat(buf.remaining()).isPositive());
    }

    @Test
    void generateInitProducerIdResponse_shouldAssignProducerIdAndEpoch() {
        final var apiVersion = ApiKeys.INIT_PRODUCER_ID.latestVersion();
        final var requestData = new InitProducerIdRequestData()
            .setTransactionalId("my-txn")
            .setTransactionTimeoutMs(60000);
        final var header = new RequestHeader(ApiKeys.INIT_PRODUCER_ID, apiVersion, "test-client", 1);

        final var response = handler.generateInitProducerIdResponse(header, serialize(requestData, apiVersion));

        final var responseData = parseInitProducerIdResponse(response, apiVersion);
        assertThat(responseData.errorCode()).isZero();
        assertThat(responseData.producerId()).isPositive();
        assertThat(responseData.producerEpoch()).isZero();
    }

    @Test
    void generateInitProducerIdResponse_subsequentCall_shouldIncrementEpoch() {
        final var apiVersion = ApiKeys.INIT_PRODUCER_ID.latestVersion();
        final var requestData = new InitProducerIdRequestData()
            .setTransactionalId("my-txn")
            .setTransactionTimeoutMs(60000);
        final var header1 = new RequestHeader(ApiKeys.INIT_PRODUCER_ID, apiVersion, "test-client", 1);
        final var header2 = new RequestHeader(ApiKeys.INIT_PRODUCER_ID, apiVersion, "test-client", 2);

        final var response1 = handler.generateInitProducerIdResponse(header1, serialize(requestData, apiVersion));
        final var response2 = handler.generateInitProducerIdResponse(header2, serialize(requestData, apiVersion));

        final var data1 = parseInitProducerIdResponse(response1, apiVersion);
        final var data2 = parseInitProducerIdResponse(response2, apiVersion);
        assertThat(data1.producerId()).isEqualTo(data2.producerId());
        assertThat(data2.producerEpoch()).isEqualTo((short) (data1.producerEpoch() + 1));
    }

    @Test
    void generateEndTxnResponse_commit_shouldReturnSuccessCode() {
        final var apiVersion = ApiKeys.END_TXN.latestVersion();
        final var requestData = new EndTxnRequestData()
            .setTransactionalId("my-txn")
            .setProducerId(1L)
            .setProducerEpoch((short) 0)
            .setCommitted(true);
        final var header = new RequestHeader(ApiKeys.END_TXN, apiVersion, "test-client", 3);

        final var response = handler.generateEndTxnResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseEndTxnResponse(response, apiVersion);
        assertThat(responseData.errorCode()).isZero();
    }

    @Test
    void generateEndTxnResponse_abort_shouldReturnSuccessCode() {
        final var apiVersion = ApiKeys.END_TXN.latestVersion();
        final var requestData = new EndTxnRequestData()
            .setTransactionalId("my-txn")
            .setProducerId(1L)
            .setProducerEpoch((short) 0)
            .setCommitted(false);
        final var header = new RequestHeader(ApiKeys.END_TXN, apiVersion, "test-client", 4);

        final var response = handler.generateEndTxnResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseEndTxnResponse(response, apiVersion);
        assertThat(responseData.errorCode()).isZero();
    }

    @Test
    void generateInitProducerIdResponse_withMalformedBuffer_shouldReturnNull() {
        final var header = new RequestHeader(ApiKeys.INIT_PRODUCER_ID,
            ApiKeys.INIT_PRODUCER_ID.latestVersion(), "test-client", 1);

        final var response = handler.generateInitProducerIdResponse(header, ByteBuffer.allocate(0));

        assertThat(response).isNull();
    }

    // --- helpers ---

    private static ByteBuffer serialize(final Message requestData, final short apiVersion) {
        final var cache = new ObjectSerializationCache();
        final var buffer = ByteBuffer.allocate(requestData.size(cache, apiVersion));
        requestData.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();
        return buffer;
    }

    private static InitProducerIdResponseData parseInitProducerIdResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.INIT_PRODUCER_ID, apiVersion);
        final var data = new InitProducerIdResponseData();
        data.read(new ByteBufferAccessor(buffer), apiVersion);
        return data;
    }

    private static EndTxnResponseData parseEndTxnResponse(final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.END_TXN, apiVersion);
        final var data = new EndTxnResponseData();
        data.read(new ByteBufferAccessor(buffer), apiVersion);
        return data;
    }

    private static void skipResponseHeader(final ByteBuffer buffer, final ApiKeys apiKey, final short apiVersion) {
        final var headerVersion = apiKey.responseHeaderVersion(apiVersion);
        final var headerBytes = headerVersion >= 1 ? 5 : 4;
        buffer.position(buffer.position() + headerBytes);
    }
}
