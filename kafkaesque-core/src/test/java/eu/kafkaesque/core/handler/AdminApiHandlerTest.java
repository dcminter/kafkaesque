package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.TopicStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AdminApiHandler}.
 */
@Slf4j
class AdminApiHandlerTest {

    private TopicStore topicStore;
    private AdminApiHandler handler;

    @BeforeEach
    void setUp() {
        topicStore = new TopicStore();
        handler = new AdminApiHandler(topicStore);
    }

    @Test
    void shouldRegisterTopicInStoreAndReturnSuccessResponse() {
        // Given
        final var response = invokeCreateTopics("new-topic", 3, (short) 1);

        // Then – topic is registered
        assertThat(topicStore.hasTopic("new-topic")).isTrue();
        assertThat(topicStore.getTopic("new-topic")).hasValueSatisfying(def -> {
            assertThat(def.numPartitions()).isEqualTo(3);
            assertThat(def.replicationFactor()).isEqualTo((short) 1);
        });

        // And – response contains a zero error code for the topic
        assertThat(response).isNotNull();
        final var responseData = parseCreateTopicsResponse(response);
        assertThat(responseData.topics()).hasSize(1);
        assertThat(responseData.topics().find("new-topic").errorCode()).isEqualTo((short) 0);
    }

    @Test
    void shouldHandleMultipleTopicsInSingleRequest() {
        // Given
        final short apiVersion = ApiKeys.CREATE_TOPICS.latestVersion();
        final var topics = new CreateTopicsRequestData.CreatableTopicCollection();
        topics.add(new CreateTopicsRequestData.CreatableTopic()
            .setName("topic-a").setNumPartitions(1).setReplicationFactor((short) 1));
        topics.add(new CreateTopicsRequestData.CreatableTopic()
            .setName("topic-b").setNumPartitions(5).setReplicationFactor((short) 1));
        final var requestData = new CreateTopicsRequestData().setTopics(topics);

        final var response = invokeCreateTopicsRequest(requestData, apiVersion);

        // Then
        assertThat(topicStore.hasTopic("topic-a")).isTrue();
        assertThat(topicStore.hasTopic("topic-b")).isTrue();
        assertThat(topicStore.getTopic("topic-b"))
            .hasValueSatisfying(def -> assertThat(def.numPartitions()).isEqualTo(5));

        final var responseData = parseCreateTopicsResponse(response);
        assertThat(responseData.topics()).hasSize(2);
    }

    @Test
    void shouldReturnNullOnMalformedRequest() {
        // Given – an empty buffer that cannot be parsed as a CreateTopicsRequest
        final var header = new RequestHeader(ApiKeys.CREATE_TOPICS,
            ApiKeys.CREATE_TOPICS.latestVersion(), "test-client", 1);
        final var emptyBuffer = ByteBuffer.allocate(0);

        // When – the following error log from AdminApiHandler is expected
        log.info("Expecting an error log from AdminApiHandler due to malformed (empty) request buffer");
        final var response = handler.generateCreateTopicsResponse(header, emptyBuffer);

        // Then
        assertThat(response).isNull();
    }

    // --- helpers ---

    private ByteBuffer invokeCreateTopics(final String name, final int partitions, final short replicationFactor) {
        final short apiVersion = ApiKeys.CREATE_TOPICS.latestVersion();
        final var topics = new CreateTopicsRequestData.CreatableTopicCollection();
        topics.add(new CreateTopicsRequestData.CreatableTopic()
            .setName(name)
            .setNumPartitions(partitions)
            .setReplicationFactor(replicationFactor));
        final var requestData = new CreateTopicsRequestData().setTopics(topics);
        return invokeCreateTopicsRequest(requestData, apiVersion);
    }

    private ByteBuffer invokeCreateTopicsRequest(final CreateTopicsRequestData requestData, final short apiVersion) {
        final var cache = new ObjectSerializationCache();
        final int bodySize = requestData.size(cache, apiVersion);
        final var buffer = ByteBuffer.allocate(bodySize);
        requestData.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();

        final var header = new RequestHeader(ApiKeys.CREATE_TOPICS, apiVersion, "test-client", 1);
        return handler.generateCreateTopicsResponse(header, buffer);
    }

    private CreateTopicsResponseData parseCreateTopicsResponse(final ByteBuffer buffer) {
        // Skip the response header (correlation id = 4 bytes for flexible encoding with tag buffer)
        final short apiVersion = ApiKeys.CREATE_TOPICS.latestVersion();
        final short headerVersion = ApiKeys.CREATE_TOPICS.responseHeaderVersion(apiVersion);
        // Flexible header has correlationId (4 bytes) + empty tag buffer (1 byte)
        final int headerBytes = (headerVersion >= 1) ? 5 : 4;
        buffer.position(buffer.position() + headerBytes);
        final var responseData = new CreateTopicsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }
}
