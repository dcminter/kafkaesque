package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.EventStore;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;

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

    @Test
    void generateListTransactionsResponse_shouldReturnRegisteredTransactions() {
        // Register a transactional producer by calling initProducerId
        final var initVersion = ApiKeys.INIT_PRODUCER_ID.latestVersion();
        final var initRequest = new InitProducerIdRequestData()
            .setTransactionalId("txn-A")
            .setTransactionTimeoutMs(60000);
        final var initHeader = new RequestHeader(ApiKeys.INIT_PRODUCER_ID, initVersion, "test-client", 1);
        handler.generateInitProducerIdResponse(initHeader, serialize(initRequest, initVersion));

        // List transactions
        final var apiVersion = ApiKeys.LIST_TRANSACTIONS.latestVersion();
        final var requestData = new ListTransactionsRequestData();
        final var header = new RequestHeader(ApiKeys.LIST_TRANSACTIONS, apiVersion, "test-client", 10);

        final var response = handler.generateListTransactionsResponse(header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseListTransactionsResponse(response, apiVersion);
        assertThat(responseData.errorCode()).isZero();
        final var txnIds = responseData.transactionStates().stream()
            .map(ListTransactionsResponseData.TransactionState::transactionalId)
            .collect(Collectors.toList());
        assertThat(txnIds).contains("txn-A");
    }

    @Test
    void generateDescribeTransactionsResponse_shouldReturnDetailsForKnownTransaction() {
        // Register a transactional producer
        final var initVersion = ApiKeys.INIT_PRODUCER_ID.latestVersion();
        final var initRequest = new InitProducerIdRequestData()
            .setTransactionalId("txn-X")
            .setTransactionTimeoutMs(60000);
        final var initHeader = new RequestHeader(ApiKeys.INIT_PRODUCER_ID, initVersion, "test-client", 1);
        handler.generateInitProducerIdResponse(initHeader, serialize(initRequest, initVersion));

        // Describe transactions
        final var apiVersion = ApiKeys.DESCRIBE_TRANSACTIONS.latestVersion();
        final var requestData = new DescribeTransactionsRequestData()
            .setTransactionalIds(java.util.List.of("txn-X", "txn-unknown"));
        final var header = new RequestHeader(
            ApiKeys.DESCRIBE_TRANSACTIONS, apiVersion, "test-client", 11);

        final var response = handler.generateDescribeTransactionsResponse(
            header, serialize(requestData, apiVersion));

        assertThat(response).isNotNull();
        final var responseData = parseDescribeTransactionsResponse(response, apiVersion);
        assertThat(responseData.transactionStates()).hasSize(2);

        final var known = responseData.transactionStates().stream()
            .filter(s -> "txn-X".equals(s.transactionalId()))
            .findFirst().orElseThrow();
        assertThat(known.producerId()).isPositive();
        assertThat(known.producerEpoch()).isZero();
        assertThat(known.transactionState()).isEqualTo("Ongoing");

        final var unknown = responseData.transactionStates().stream()
            .filter(s -> "txn-unknown".equals(s.transactionalId()))
            .findFirst().orElseThrow();
        assertThat(unknown.producerId()).isEqualTo(-1L);
        assertThat(unknown.transactionState()).isEmpty();
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

    private static ListTransactionsResponseData parseListTransactionsResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.LIST_TRANSACTIONS, apiVersion);
        final var data = new ListTransactionsResponseData();
        data.read(new ByteBufferAccessor(buffer), apiVersion);
        return data;
    }

    private static DescribeTransactionsResponseData parseDescribeTransactionsResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.DESCRIBE_TRANSACTIONS, apiVersion);
        final var data = new DescribeTransactionsResponseData();
        data.read(new ByteBufferAccessor(buffer), apiVersion);
        return data;
    }

    private static void skipResponseHeader(final ByteBuffer buffer, final ApiKeys apiKey, final short apiVersion) {
        final var headerVersion = apiKey.responseHeaderVersion(apiVersion);
        final var headerBytes = headerVersion >= 1 ? 5 : 4;
        buffer.position(buffer.position() + headerBytes);
    }
}
