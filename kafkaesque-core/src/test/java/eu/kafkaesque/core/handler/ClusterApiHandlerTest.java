package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.TopicStore;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ClusterApiHandler}.
 */
class ClusterApiHandlerTest {

    private TopicStore topicStore;
    private ClusterApiHandler handler;

    @BeforeEach
    void setUp() {
        topicStore = new TopicStore();
        handler = new ClusterApiHandler(null, topicStore);
    }

    @Test
    void generateApiVersionsResponse_shouldReturnNonNullBuffer() {
        final var header = new RequestHeader(ApiKeys.API_VERSIONS,
            ApiKeys.API_VERSIONS.latestVersion(), "test-client", 1);

        final var response = handler.generateApiVersionsResponse(header);

        assertThat(response).isNotNull().satisfies(buf -> assertThat(buf.remaining()).isPositive());
    }

    @Test
    void generateApiVersionsResponse_shouldContainSupportedApiKeys() {
        final var header = new RequestHeader(ApiKeys.API_VERSIONS,
            ApiKeys.API_VERSIONS.latestVersion(), "test-client", 1);

        final var response = handler.generateApiVersionsResponse(header);

        final var responseData = parseApiVersionsResponse(response, header.apiVersion());
        assertThat(responseData.errorCode()).isZero();
        assertThat(responseData.apiKeys()).isNotEmpty();
    }

    @Test
    void generateDescribeClusterResponse_shouldReturnNonNullBuffer() {
        final var header = new RequestHeader(ApiKeys.DESCRIBE_CLUSTER,
            ApiKeys.DESCRIBE_CLUSTER.latestVersion(), "test-client", 2);

        final var response = handler.generateDescribeClusterResponse(header);

        assertThat(response).isNotNull().satisfies(buf -> assertThat(buf.remaining()).isPositive());
    }

    @Test
    void generateDescribeClusterResponse_shouldContainClusterDetails() {
        final var header = new RequestHeader(ApiKeys.DESCRIBE_CLUSTER,
            ApiKeys.DESCRIBE_CLUSTER.latestVersion(), "test-client", 2);

        final var response = handler.generateDescribeClusterResponse(header);

        final var responseData = parseDescribeClusterResponse(response, header.apiVersion());
        assertThat(responseData.errorCode()).isZero();
        assertThat(responseData.clusterId()).isNotBlank();
        assertThat(responseData.brokers()).isNotEmpty();
    }

    @Test
    void generateMetadataResponse_shouldReturnNonNullBuffer() {
        final var apiVersion = ApiKeys.METADATA.latestVersion();
        final var requestData = new MetadataRequestData();
        final var header = new RequestHeader(ApiKeys.METADATA, apiVersion, "test-client", 3);

        final var response = handler.generateMetadataResponse(header, serializeRequest(requestData, apiVersion));

        assertThat(response).isNotNull().satisfies(buf -> assertThat(buf.remaining()).isPositive());
    }

    @Test
    void generateMetadataResponse_shouldIncludeRegisteredTopics() {
        topicStore.createTopic("existing-topic", 3, (short) 1);
        final var apiVersion = ApiKeys.METADATA.latestVersion();
        // Request the specific topic by name
        final var topicRequest = new MetadataRequestData.MetadataRequestTopic().setName("existing-topic");
        final var requestData = new MetadataRequestData()
            .setTopics(java.util.List.of(topicRequest));
        final var header = new RequestHeader(ApiKeys.METADATA, apiVersion, "test-client", 3);

        final var response = handler.generateMetadataResponse(header, serializeRequest(requestData, apiVersion));

        final var responseData = parseMetadataResponse(response, apiVersion);
        assertThat(responseData.topics().stream().map(MetadataResponseData.MetadataResponseTopic::name))
            .contains("existing-topic");
    }

    @Test
    void generateFindCoordinatorResponse_shouldReturnNonNullBuffer() {
        final var apiVersion = (short) 4; // v4+ uses coordinatorKeys
        final var requestData = new FindCoordinatorRequestData()
            .setKeyType((byte) 0)
            .setCoordinatorKeys(java.util.List.of("my-group"));
        final var header = new RequestHeader(ApiKeys.FIND_COORDINATOR, apiVersion, "test-client", 4);

        final var response = handler.generateFindCoordinatorResponse(
            header, serializeRequest(requestData, apiVersion));

        assertThat(response).isNotNull().satisfies(buf -> assertThat(buf.remaining()).isPositive());
    }

    @Test
    void generateFindCoordinatorResponse_shouldReturnZeroErrorCode() {
        final var apiVersion = (short) 4; // v4+ uses coordinatorKeys
        final var requestData = new FindCoordinatorRequestData()
            .setKeyType((byte) 0)
            .setCoordinatorKeys(java.util.List.of("my-group"));
        final var header = new RequestHeader(ApiKeys.FIND_COORDINATOR, apiVersion, "test-client", 4);

        final var response = handler.generateFindCoordinatorResponse(
            header, serializeRequest(requestData, apiVersion));

        final var responseData = parseFindCoordinatorResponse(response, apiVersion);
        // For version >= 4, error code is on each coordinator entry
        assertThat(responseData.coordinators()).hasSize(1);
        assertThat(responseData.coordinators().get(0).errorCode()).isZero();
    }

    // --- helpers ---

    private static ByteBuffer serializeRequest(
            final org.apache.kafka.common.protocol.Message requestData, final short apiVersion) {
        final var cache = new ObjectSerializationCache();
        final var buffer = ByteBuffer.allocate(requestData.size(cache, apiVersion));
        requestData.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();
        return buffer;
    }

    private static ApiVersionsResponseData parseApiVersionsResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.API_VERSIONS, apiVersion);
        final var responseData = new ApiVersionsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private static DescribeClusterResponseData parseDescribeClusterResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.DESCRIBE_CLUSTER, apiVersion);
        final var responseData = new DescribeClusterResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private static MetadataResponseData parseMetadataResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.METADATA, apiVersion);
        final var responseData = new MetadataResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private static FindCoordinatorResponseData parseFindCoordinatorResponse(
            final ByteBuffer buffer, final short apiVersion) {
        skipResponseHeader(buffer, ApiKeys.FIND_COORDINATOR, apiVersion);
        final var responseData = new FindCoordinatorResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private static void skipResponseHeader(final ByteBuffer buffer, final ApiKeys apiKey, final short apiVersion) {
        final var headerVersion = apiKey.responseHeaderVersion(apiVersion);
        final var headerBytes = headerVersion >= 1 ? 5 : 4;
        buffer.position(buffer.position() + headerBytes);
    }
}
