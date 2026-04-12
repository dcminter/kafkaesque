package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.EventStore;
import eu.kafkaesque.core.storage.TopicStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AdminApiHandler}.
 */
@Slf4j
class AdminApiHandlerTest {

    private TopicStore topicStore;
    private EventStore eventStore;
    private AdminApiHandler handler;

    @BeforeEach
    void setUp() {
        topicStore = new TopicStore();
        eventStore = new EventStore();
        handler = new AdminApiHandler(topicStore, eventStore);
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

    @Test
    void shouldDescribeTopicConfigWithExpectedEntries() {
        // Given
        invokeCreateTopics("config-topic", 3, (short) 1);

        // When
        final var response = invokeDescribeConfigs((byte) 2, "config-topic");

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseDescribeConfigsResponse(response);
        assertThat(responseData.results()).hasSize(1);
        final var result = responseData.results().get(0);
        assertThat(result.errorCode()).isEqualTo((short) 0);

        final var configNames = result.configs().stream()
            .map(DescribeConfigsResponseData.DescribeConfigsResourceResult::name)
            .collect(Collectors.toList());
        assertThat(configNames).contains("cleanup.policy", "retention.ms", "retention.bytes", "compression.type");
    }

    @Test
    void shouldReturnEmptyConfigsForNonExistentTopic() {
        // When
        final var response = invokeDescribeConfigs((byte) 2, "no-such-topic");

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseDescribeConfigsResponse(response);
        assertThat(responseData.results()).hasSize(1);
        assertThat(responseData.results().get(0).configs()).isEmpty();
    }

    @Test
    void shouldDeleteExistingTopicFromStoreAndReturnSuccessResponse() {
        // Given
        invokeCreateTopics("doomed-topic", 2, (short) 1);
        assertThat(topicStore.hasTopic("doomed-topic")).isTrue();

        // When
        final var response = invokeDeleteTopics("doomed-topic");

        // Then
        assertThat(topicStore.hasTopic("doomed-topic")).isFalse();
        assertThat(response).isNotNull();
        final var responseData = parseDeleteTopicsResponse(response);
        assertThat(responseData.responses()).hasSize(1);
        assertThat(responseData.responses().find("doomed-topic").errorCode()).isEqualTo((short) 0);
    }

    @Test
    void shouldReturnSuccessForDeletionOfNonExistentTopic() {
        // When
        final var response = invokeDeleteTopics("ghost-topic");

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseDeleteTopicsResponse(response);
        assertThat(responseData.responses()).hasSize(1);
        assertThat(responseData.responses().find("ghost-topic").errorCode()).isEqualTo((short) 0);
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

    private ByteBuffer invokeDescribeConfigs(final byte resourceType, final String resourceName) {
        final short apiVersion = ApiKeys.DESCRIBE_CONFIGS.latestVersion();
        final var resources = java.util.List.of(
            new DescribeConfigsRequestData.DescribeConfigsResource()
                .setResourceType(resourceType)
                .setResourceName(resourceName));
        final var requestData = new DescribeConfigsRequestData().setResources(resources);

        final var cache = new ObjectSerializationCache();
        final int bodySize = requestData.size(cache, apiVersion);
        final var buffer = ByteBuffer.allocate(bodySize);
        requestData.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();

        final var header = new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, apiVersion, "test-client", 1);
        return handler.generateDescribeConfigsResponse(header, buffer);
    }

    private DescribeConfigsResponseData parseDescribeConfigsResponse(final ByteBuffer buffer) {
        final short apiVersion = ApiKeys.DESCRIBE_CONFIGS.latestVersion();
        final short headerVersion = ApiKeys.DESCRIBE_CONFIGS.responseHeaderVersion(apiVersion);
        final int headerBytes = (headerVersion >= 1) ? 5 : 4;
        buffer.position(buffer.position() + headerBytes);
        final var responseData = new DescribeConfigsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private ByteBuffer invokeDeleteTopics(final String name) {
        final short apiVersion = ApiKeys.DELETE_TOPICS.latestVersion();
        final var topics = java.util.List.of(
            new DeleteTopicsRequestData.DeleteTopicState().setName(name));
        final var requestData = new DeleteTopicsRequestData().setTopics(topics);

        final var cache = new ObjectSerializationCache();
        final int bodySize = requestData.size(cache, apiVersion);
        final var buffer = ByteBuffer.allocate(bodySize);
        requestData.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();

        final var header = new RequestHeader(ApiKeys.DELETE_TOPICS, apiVersion, "test-client", 1);
        return handler.generateDeleteTopicsResponse(header, buffer);
    }

    private DeleteTopicsResponseData parseDeleteTopicsResponse(final ByteBuffer buffer) {
        final short apiVersion = ApiKeys.DELETE_TOPICS.latestVersion();
        final short headerVersion = ApiKeys.DELETE_TOPICS.responseHeaderVersion(apiVersion);
        final int headerBytes = (headerVersion >= 1) ? 5 : 4;
        buffer.position(buffer.position() + headerBytes);
        final var responseData = new DeleteTopicsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
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

    // =====================================================================
    // ALTER_CONFIGS tests
    // =====================================================================

    @Test
    void shouldReturnSuccessForAlterConfigs() {
        // Given
        invokeCreateTopics("alter-config-topic", 1, (short) 1);

        // When
        final var response = invokeAlterConfigs("alter-config-topic");

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseAlterConfigsResponse(response);
        assertThat(responseData.responses()).hasSize(1);
        final var result = responseData.responses().get(0);
        assertThat(result.errorCode()).isEqualTo((short) 0);
        assertThat(result.resourceName()).isEqualTo("alter-config-topic");
    }

    // =====================================================================
    // CREATE_PARTITIONS tests
    // =====================================================================

    @Test
    void shouldIncreasePartitionCountAndReturnSuccess() {
        // Given
        invokeCreateTopics("partition-topic", 2, (short) 1);

        // When
        final var response = invokeCreatePartitions("partition-topic", 5);

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseCreatePartitionsResponse(response);
        assertThat(responseData.results()).hasSize(1);
        assertThat(responseData.results().get(0).errorCode()).isEqualTo((short) 0);

        assertThat(topicStore.getTopic("partition-topic"))
            .hasValueSatisfying(def -> assertThat(def.numPartitions()).isEqualTo(5));
    }

    @Test
    void shouldReturnSuccessForCreatePartitionsOnNonExistentTopic() {
        // When
        final var response = invokeCreatePartitions("ghost-topic", 3);

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseCreatePartitionsResponse(response);
        assertThat(responseData.results()).hasSize(1);
        assertThat(responseData.results().get(0).errorCode()).isEqualTo((short) 0);
    }

    // =====================================================================
    // DELETE_RECORDS tests
    // =====================================================================

    @Test
    void shouldDeleteRecordsBeforeOffsetAndReturnLowWatermark() {
        // Given — store records
        final var topic = "delete-records-topic";
        invokeCreateTopics(topic, 1, (short) 1);
        for (int i = 0; i < 5; i++) {
            eventStore.storeRecord(topic, 0, System.currentTimeMillis(), "key-" + i, "val-" + i);
        }

        // When — delete before offset 3
        final var response = invokeDeleteRecords(topic, 0, 3L);

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseDeleteRecordsResponse(response);
        final var topicResults = responseData.topics();
        assertThat(topicResults).hasSize(1);
        final var partitionResult = topicResults.iterator().next().partitions().iterator().next();
        assertThat(partitionResult.errorCode()).isEqualTo((short) 0);
        assertThat(partitionResult.lowWatermark()).isGreaterThanOrEqualTo(3L);
    }

    // =====================================================================
    // INCREMENTAL_ALTER_CONFIGS tests
    // =====================================================================

    @Test
    void shouldReturnSuccessForIncrementalAlterConfigs() {
        // Given
        invokeCreateTopics("inc-alter-topic", 1, (short) 1);

        // When
        final var response = invokeIncrementalAlterConfigs("inc-alter-topic");

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseIncrementalAlterConfigsResponse(response);
        assertThat(responseData.responses()).hasSize(1);
        final var result = responseData.responses().get(0);
        assertThat(result.errorCode()).isEqualTo((short) 0);
        assertThat(result.resourceName()).isEqualTo("inc-alter-topic");
    }

    @Test
    void shouldHandleMultipleResourcesInIncrementalAlterConfigs() {
        // Given
        invokeCreateTopics("topic-a", 1, (short) 1);
        invokeCreateTopics("topic-b", 1, (short) 1);

        // When
        final short apiVersion = ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion();
        final var resources = new IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection();
        resources.add(new IncrementalAlterConfigsRequestData.AlterConfigsResource()
            .setResourceType((byte) 2).setResourceName("topic-a"));
        resources.add(new IncrementalAlterConfigsRequestData.AlterConfigsResource()
            .setResourceType((byte) 2).setResourceName("topic-b"));
        final var requestData = new IncrementalAlterConfigsRequestData().setResources(resources);

        final var buffer = serialize(requestData, apiVersion);
        final var header = new RequestHeader(
            ApiKeys.INCREMENTAL_ALTER_CONFIGS, apiVersion, "test-client", 1);
        final var response = handler.generateIncrementalAlterConfigsResponse(header, buffer);

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseIncrementalAlterConfigsResponse(response);
        assertThat(responseData.responses()).hasSize(2);
    }

    // =====================================================================
    // ELECT_LEADERS tests
    // =====================================================================

    @Test
    void shouldReturnElectionNotNeededForKnownPartitions() {
        // Given
        invokeCreateTopics("elect-topic", 2, (short) 1);

        // When
        final var response = invokeElectLeaders("elect-topic", of(0, 1));

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseElectLeadersResponse(response);
        assertThat(responseData.errorCode()).isEqualTo((short) 0);
        assertThat(responseData.replicaElectionResults()).hasSize(1);
        final var partitionResults = responseData.replicaElectionResults().get(0).partitionResult();
        assertThat(partitionResults).hasSize(2);
        assertThat(partitionResults).allSatisfy(
            p -> assertThat(p.errorCode()).isEqualTo(Errors.ELECTION_NOT_NEEDED.code()));
    }

    @Test
    void shouldReturnEmptyResultsWhenTopicPartitionsIsNull() {
        // When — request with null topicPartitions (elect all)
        final short apiVersion = ApiKeys.ELECT_LEADERS.latestVersion();
        final var requestData = new ElectLeadersRequestData()
            .setTopicPartitions(null)
            .setElectionType((byte) 0);

        final var buffer = serialize(requestData, apiVersion);
        final var header = new RequestHeader(ApiKeys.ELECT_LEADERS, apiVersion, "test-client", 1);
        final var response = handler.generateElectLeadersResponse(header, buffer);

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseElectLeadersResponse(response);
        assertThat(responseData.replicaElectionResults()).isEmpty();
    }

    // =====================================================================
    // DESCRIBE_LOG_DIRS tests
    // =====================================================================

    @Test
    void shouldReturnSyntheticLogDirWithRegisteredTopics() {
        // Given
        invokeCreateTopics("log-dir-topic-a", 2, (short) 1);
        invokeCreateTopics("log-dir-topic-b", 3, (short) 1);

        // When
        final var response = invokeDescribeLogDirs();

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseDescribeLogDirsResponse(response);
        assertThat(responseData.results()).hasSize(1);

        final var logDir = responseData.results().get(0);
        assertThat(logDir.logDir()).isEqualTo("/kafkaesque");
        assertThat(logDir.errorCode()).isEqualTo((short) 0);

        final var topicNames = logDir.topics().stream()
            .map(DescribeLogDirsResponseData.DescribeLogDirsTopic::name)
            .collect(Collectors.toList());
        assertThat(topicNames).containsExactlyInAnyOrder("log-dir-topic-a", "log-dir-topic-b");
    }

    @Test
    void shouldReturnEmptyTopicsWhenNoneRegistered() {
        // When
        final var response = invokeDescribeLogDirs();

        // Then
        final var responseData = parseDescribeLogDirsResponse(response);
        assertThat(responseData.results()).hasSize(1);
        assertThat(responseData.results().get(0).topics()).isEmpty();
    }

    // =====================================================================
    // ALTER_REPLICA_LOG_DIRS tests
    // =====================================================================

    @Test
    void shouldReturnSuccessForAlterReplicaLogDirs() {
        // Given
        invokeCreateTopics("replica-topic", 2, (short) 1);

        // When
        final var response = invokeAlterReplicaLogDirs("replica-topic", of(0, 1));

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseAlterReplicaLogDirsResponse(response);
        assertThat(responseData.results()).hasSize(1);
        final var topicResult = responseData.results().get(0);
        assertThat(topicResult.topicName()).isEqualTo("replica-topic");
        assertThat(topicResult.partitions()).hasSize(2);
        assertThat(topicResult.partitions()).allSatisfy(
            p -> assertThat(p.errorCode()).isEqualTo((short) 0));
    }

    // =====================================================================
    // Additional helpers
    // =====================================================================

    private static ByteBuffer serialize(
            final org.apache.kafka.common.protocol.ApiMessage message, final short apiVersion) {
        final var cache = new ObjectSerializationCache();
        final int bodySize = message.size(cache, apiVersion);
        final var buffer = ByteBuffer.allocate(bodySize);
        message.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();
        return buffer;
    }

    private static void skipResponseHeader(
            final ByteBuffer buffer, final ApiKeys apiKey, final short apiVersion) {
        final short headerVersion = apiKey.responseHeaderVersion(apiVersion);
        final int headerBytes = (headerVersion >= 1) ? 5 : 4;
        buffer.position(buffer.position() + headerBytes);
    }

    private ByteBuffer invokeAlterConfigs(final String topicName) {
        final short apiVersion = ApiKeys.ALTER_CONFIGS.latestVersion();
        final var resources = new AlterConfigsRequestData.AlterConfigsResourceCollection();
        resources.add(new AlterConfigsRequestData.AlterConfigsResource()
            .setResourceType((byte) 2)
            .setResourceName(topicName));
        final var requestData = new AlterConfigsRequestData().setResources(resources);
        final var buffer = serialize(requestData, apiVersion);
        final var header = new RequestHeader(ApiKeys.ALTER_CONFIGS, apiVersion, "test-client", 1);
        return handler.generateAlterConfigsResponse(header, buffer);
    }

    private AlterConfigsResponseData parseAlterConfigsResponse(final ByteBuffer buffer) {
        final short apiVersion = ApiKeys.ALTER_CONFIGS.latestVersion();
        skipResponseHeader(buffer, ApiKeys.ALTER_CONFIGS, apiVersion);
        final var responseData = new AlterConfigsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private ByteBuffer invokeCreatePartitions(final String topicName, final int count) {
        final short apiVersion = ApiKeys.CREATE_PARTITIONS.latestVersion();
        final var topics = new CreatePartitionsRequestData.CreatePartitionsTopicCollection();
        topics.add(new CreatePartitionsRequestData.CreatePartitionsTopic()
            .setName(topicName)
            .setCount(count));
        final var requestData = new CreatePartitionsRequestData().setTopics(topics);
        final var buffer = serialize(requestData, apiVersion);
        final var header = new RequestHeader(
            ApiKeys.CREATE_PARTITIONS, apiVersion, "test-client", 1);
        return handler.generateCreatePartitionsResponse(header, buffer);
    }

    private CreatePartitionsResponseData parseCreatePartitionsResponse(final ByteBuffer buffer) {
        final short apiVersion = ApiKeys.CREATE_PARTITIONS.latestVersion();
        skipResponseHeader(buffer, ApiKeys.CREATE_PARTITIONS, apiVersion);
        final var responseData = new CreatePartitionsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private ByteBuffer invokeDeleteRecords(
            final String topicName, final int partition, final long beforeOffset) {
        final short apiVersion = ApiKeys.DELETE_RECORDS.latestVersion();
        final var partitionEntry = new DeleteRecordsRequestData.DeleteRecordsPartition()
            .setPartitionIndex(partition)
            .setOffset(beforeOffset);
        final var topicEntry = new DeleteRecordsRequestData.DeleteRecordsTopic()
            .setName(topicName)
            .setPartitions(of(partitionEntry));
        final var requestData = new DeleteRecordsRequestData().setTopics(of(topicEntry));
        final var buffer = serialize(requestData, apiVersion);
        final var header = new RequestHeader(ApiKeys.DELETE_RECORDS, apiVersion, "test-client", 1);
        return handler.generateDeleteRecordsResponse(header, buffer);
    }

    private DeleteRecordsResponseData parseDeleteRecordsResponse(final ByteBuffer buffer) {
        final short apiVersion = ApiKeys.DELETE_RECORDS.latestVersion();
        skipResponseHeader(buffer, ApiKeys.DELETE_RECORDS, apiVersion);
        final var responseData = new DeleteRecordsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private ByteBuffer invokeIncrementalAlterConfigs(final String topicName) {
        final short apiVersion = ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion();
        final var resources = new IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection();
        resources.add(new IncrementalAlterConfigsRequestData.AlterConfigsResource()
            .setResourceType((byte) 2)
            .setResourceName(topicName));
        final var requestData = new IncrementalAlterConfigsRequestData().setResources(resources);
        final var buffer = serialize(requestData, apiVersion);
        final var header = new RequestHeader(
            ApiKeys.INCREMENTAL_ALTER_CONFIGS, apiVersion, "test-client", 1);
        return handler.generateIncrementalAlterConfigsResponse(header, buffer);
    }

    private IncrementalAlterConfigsResponseData parseIncrementalAlterConfigsResponse(
            final ByteBuffer buffer) {
        final short apiVersion = ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion();
        skipResponseHeader(buffer, ApiKeys.INCREMENTAL_ALTER_CONFIGS, apiVersion);
        final var responseData = new IncrementalAlterConfigsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private ByteBuffer invokeElectLeaders(final String topicName, final List<Integer> partitions) {
        final short apiVersion = ApiKeys.ELECT_LEADERS.latestVersion();
        final var tpCollection = new ElectLeadersRequestData.TopicPartitionsCollection();
        tpCollection.add(new ElectLeadersRequestData.TopicPartitions()
            .setTopic(topicName)
            .setPartitions(partitions));
        final var requestData = new ElectLeadersRequestData()
            .setTopicPartitions(tpCollection)
            .setElectionType((byte) 0);
        final var buffer = serialize(requestData, apiVersion);
        final var header = new RequestHeader(ApiKeys.ELECT_LEADERS, apiVersion, "test-client", 1);
        return handler.generateElectLeadersResponse(header, buffer);
    }

    private ElectLeadersResponseData parseElectLeadersResponse(final ByteBuffer buffer) {
        final short apiVersion = ApiKeys.ELECT_LEADERS.latestVersion();
        skipResponseHeader(buffer, ApiKeys.ELECT_LEADERS, apiVersion);
        final var responseData = new ElectLeadersResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private ByteBuffer invokeDescribeLogDirs() {
        final short apiVersion = ApiKeys.DESCRIBE_LOG_DIRS.latestVersion();
        final var requestData = new DescribeLogDirsRequestData();
        final var buffer = serialize(requestData, apiVersion);
        final var header = new RequestHeader(
            ApiKeys.DESCRIBE_LOG_DIRS, apiVersion, "test-client", 1);
        return handler.generateDescribeLogDirsResponse(header, buffer);
    }

    private DescribeLogDirsResponseData parseDescribeLogDirsResponse(final ByteBuffer buffer) {
        final short apiVersion = ApiKeys.DESCRIBE_LOG_DIRS.latestVersion();
        skipResponseHeader(buffer, ApiKeys.DESCRIBE_LOG_DIRS, apiVersion);
        final var responseData = new DescribeLogDirsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private ByteBuffer invokeAlterReplicaLogDirs(
            final String topicName, final List<Integer> partitions) {
        final short apiVersion = ApiKeys.ALTER_REPLICA_LOG_DIRS.latestVersion();
        final var topicCollection =
            new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopicCollection();
        topicCollection.add(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic()
            .setName(topicName)
            .setPartitions(partitions));
        final var dirCollection = new AlterReplicaLogDirsRequestData.AlterReplicaLogDirCollection();
        dirCollection.add(new AlterReplicaLogDirsRequestData.AlterReplicaLogDir()
            .setPath("/kafkaesque")
            .setTopics(topicCollection));
        final var requestData = new AlterReplicaLogDirsRequestData().setDirs(dirCollection);
        final var buffer = serialize(requestData, apiVersion);
        final var header = new RequestHeader(
            ApiKeys.ALTER_REPLICA_LOG_DIRS, apiVersion, "test-client", 1);
        return handler.generateAlterReplicaLogDirsResponse(header, buffer);
    }

    private AlterReplicaLogDirsResponseData parseAlterReplicaLogDirsResponse(
            final ByteBuffer buffer) {
        final short apiVersion = ApiKeys.ALTER_REPLICA_LOG_DIRS.latestVersion();
        skipResponseHeader(buffer, ApiKeys.ALTER_REPLICA_LOG_DIRS, apiVersion);
        final var responseData = new AlterReplicaLogDirsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }
}
