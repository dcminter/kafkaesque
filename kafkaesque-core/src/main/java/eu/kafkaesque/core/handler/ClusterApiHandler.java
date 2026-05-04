package eu.kafkaesque.core.handler;

import edu.umd.cs.findbugs.annotations.Nullable;
import eu.kafkaesque.core.ServerInfo;
import eu.kafkaesque.core.storage.TopicStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import static java.util.Arrays.stream;
import static java.util.List.of;
import static java.util.stream.Collectors.toList;

/**
 * Handles Kafka cluster and topology API responses.
 *
 * <p>Covers {@link ApiKeys#API_VERSIONS}, {@link ApiKeys#METADATA},
 * {@link ApiKeys#DESCRIBE_CLUSTER}, {@link ApiKeys#FIND_COORDINATOR}, and
 * {@link ApiKeys#DESCRIBE_TOPIC_PARTITIONS}.</p>
 *
 * @see KafkaProtocolHandler
 */
@Slf4j
@RequiredArgsConstructor
final class ClusterApiHandler {

    private static final String CLUSTER_ID = "kafkaesque-cluster-001";
    private static final int DEFAULT_PORT = 9092;
    private static final String DEFAULT_HOST = "localhost";

    /**
     * Maximum advertised FETCH API version.
     *
     * <p>FETCH v13+ replaced topic names with topic UUIDs. Capping at v12 ensures
     * clients always send topic names, which is all the mock needs to look up records.</p>
     */
    private static final short FETCH_MAX_VERSION = 12;

    @Nullable
    private final ServerInfo serverInfo;
    private final TopicStore topicStore;
    private final boolean autoCreateTopicsEnabled;

    /**
     * Creates a new handler backed by the given server info and topic store,
     * with auto-topic-creation enabled.
     *
     * @param serverInfo the server info used to advertise host and port in cluster responses;
     *                   may be {@code null} to use built-in defaults
     * @param topicStore the topic registry consulted when building metadata responses
     */
    ClusterApiHandler(@Nullable final ServerInfo serverInfo, final TopicStore topicStore) {
        this(serverInfo, topicStore, true);
    }

    /**
     * Generates an API_VERSIONS response listing all supported Kafka APIs.
     *
     * <p>This response is special-cased because the Kafka clients send {@code API_VERSIONS}
     * before they know which version of the API to speak; the response must therefore be
     * serialised manually rather than through the regular response-header machinery.</p>
     *
     * @param requestHeader the request header
     * @return the serialised response buffer
     */
    ByteBuffer generateApiVersionsResponse(final RequestHeader requestHeader) {
        final var data = new ApiVersionsResponseData()
            .setErrorCode((short) 0)
            .setThrottleTimeMs(0)
            .setApiKeys(buildSupportedApiVersions());

        final var buffer = ByteBuffer.allocate(8192);
        buffer.putInt(requestHeader.correlationId());

        final var cache = new ObjectSerializationCache();
        data.write(new ByteBufferAccessor(buffer), cache, requestHeader.apiVersion());

        buffer.flip();
        return buffer;
    }

    /**
     * Generates a METADATA response for the requested topics.
     *
     * <p>When the request's topic list is {@code null} (i.e. the client wants all
     * topics, as used by {@code AdminClient.listTopics()}), all topics registered in
     * the {@link TopicStore} are returned. When specific topics are requested, each
     * is returned with the partition count stored in the {@link TopicStore}, falling
     * back to a single partition for topics that were not explicitly created via the
     * admin API (e.g. auto-created by a producer).</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed METADATA request
     * @return the serialised response buffer
     */
    ByteBuffer generateMetadataResponse(final RequestHeader requestHeader, final MetadataRequest request) {
        final var metadataRequest = request.data();

        final var data = new MetadataResponseData();

        final var brokers = new MetadataResponseData.MetadataResponseBrokerCollection();
        brokers.add(createMetadataBroker());
        data.setBrokers(brokers);
        data.setClusterId(CLUSTER_ID);
        data.setControllerId(1);

        final var topics = new MetadataResponseData.MetadataResponseTopicCollection();
        final var requestedTopics = metadataRequest.topics();

        if (requestedTopics == null) {
            // null means "list all topics"
            if (topicStore != null) {
                topicStore.getTopics().stream()
                    .map(topic -> buildMetadataTopicResponse(topic.name(), topic.numPartitions()))
                    .forEach(topics::add);
            }
        } else if (!requestedTopics.isEmpty()) {
            requestedTopics.stream()
                .filter(t -> t.name() != null && !t.name().isEmpty())
                .map(t -> autoCreateTopicsEnabled || (topicStore != null && topicStore.hasTopic(t.name()))
                    ? buildMetadataTopicResponse(t.name(), resolvePartitionCount(t.name()))
                    : buildMetadataTopicErrorResponse(t.name()))
                .forEach(topics::add);
        }

        data.setTopics(topics);
        return ResponseSerializer.serialize(requestHeader, data, ApiKeys.METADATA);
    }

    /**
     * Generates a DESCRIBE_CLUSTER response.
     *
     * @param requestHeader the request header
     * @return the serialised response buffer
     */
    ByteBuffer generateDescribeClusterResponse(final RequestHeader requestHeader) {
        final var brokers = new DescribeClusterResponseData.DescribeClusterBrokerCollection();
        brokers.add(createDescribeClusterBroker());

        final var data = new DescribeClusterResponseData()
            .setClusterId(CLUSTER_ID)
            .setControllerId(1)
            .setBrokers(brokers);

        return ResponseSerializer.serialize(requestHeader, data, ApiKeys.DESCRIBE_CLUSTER);
    }

    /**
     * Generates a FIND_COORDINATOR response returning this broker as the group coordinator.
     *
     * <p>API version 4 and above use a {@code coordinators} array; earlier versions use
     * a single set of top-level fields.</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed FIND_COORDINATOR request
     * @return the serialised response buffer
     */
    ByteBuffer generateFindCoordinatorResponse(final RequestHeader requestHeader, final FindCoordinatorRequest request) {
        final var requestData = request.data();

        final var data = new FindCoordinatorResponseData().setThrottleTimeMs(0);

        if (requestHeader.apiVersion() >= 4) {
            final var coordinators = requestData.coordinatorKeys().stream()
                .map(key -> new FindCoordinatorResponseData.Coordinator()
                    .setKey(key)
                    .setNodeId(1)
                    .setHost(getServerHost())
                    .setPort(getServerPort())
                    .setErrorCode((short) 0))
                .collect(toList());
            data.setCoordinators(coordinators);
        } else {
            data.setErrorCode((short) 0)
                .setNodeId(1)
                .setHost(getServerHost())
                .setPort(getServerPort());
        }

        return ResponseSerializer.serialize(requestHeader, data, ApiKeys.FIND_COORDINATOR);
    }

    /**
     * Generates a DESCRIBE_TOPIC_PARTITIONS response for the requested topics.
     *
     * <p>Each topic is looked up in the {@link TopicStore}. Topics that are registered
     * are returned with their full partition details. Topics that are not registered
     * receive an {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} error code and no partitions.
     * Pagination via the request cursor is not supported; {@code nextCursor} is always
     * null in the response.</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed DESCRIBE_TOPIC_PARTITIONS request
     * @return the serialised response buffer
     */
    ByteBuffer generateDescribeTopicPartitionsResponse(
            final RequestHeader requestHeader, final DescribeTopicPartitionsRequest request) {
        final var requestData = request.data();

        final var responseTopics = new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopicCollection();

        requestData.topics().stream()
            .map(topicRequest -> buildDescribeTopicPartitionsTopicResponse(topicRequest.name()))
            .forEach(responseTopics::add);

        final var data = new DescribeTopicPartitionsResponseData()
            .setThrottleTimeMs(0)
            .setTopics(responseTopics)
            .setNextCursor(null);

        return ResponseSerializer.serialize(requestHeader, data, ApiKeys.DESCRIBE_TOPIC_PARTITIONS);
    }

    /**
     * Builds the collection of supported API versions, applying version caps where
     * the mock does not support the full range advertised by the Kafka library.
     *
     * @return the collection of supported API version entries
     */
    private ApiVersionsResponseData.ApiVersionCollection buildSupportedApiVersions() {
        final var apiVersions = new ApiVersionsResponseData.ApiVersionCollection();

        stream(ApiKeys.values())
            .filter(apiKey -> apiKey != ApiKeys.CONTROLLED_SHUTDOWN
                && apiKey != ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS
                && apiKey != ApiKeys.PUSH_TELEMETRY
                && apiKey.id >= 0)
            .map(apiKey -> new ApiVersionsResponseData.ApiVersion()
                .setApiKey(apiKey.id)
                .setMinVersion(apiKey.oldestVersion())
                .setMaxVersion(apiKey == ApiKeys.FETCH ? FETCH_MAX_VERSION : apiKey.latestVersion()))
            .forEach(apiVersions::add);

        return apiVersions;
    }

    /**
     * Builds a DESCRIBE_TOPIC_PARTITIONS topic entry for the given topic name.
     *
     * <p>If the topic is registered in the {@link TopicStore}, a full partition listing
     * is included. If it is unknown, an {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} error
     * is returned with an empty partition list.</p>
     *
     * @param topicName the topic name to describe
     * @return the response topic entry
     */
    private DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic buildDescribeTopicPartitionsTopicResponse(
            final String topicName) {

        if (topicStore == null || !topicStore.hasTopic(topicName)) {
            return new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic()
                .setName(topicName)
                .setTopicId(Uuid.ZERO_UUID)
                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                .setIsInternal(false)
                .setPartitions(of())
                .setTopicAuthorizedOperations(Integer.MIN_VALUE);
        }

        final var definition = topicStore.getTopic(topicName).orElseThrow();
        final var partitions = IntStream.range(0, definition.numPartitions())
            .mapToObj(i -> new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition()
                .setErrorCode((short) 0)
                .setPartitionIndex(i)
                .setLeaderId(1)
                .setLeaderEpoch(0)
                .setReplicaNodes(of(1))
                .setIsrNodes(of(1))
                .setEligibleLeaderReplicas(of())
                .setLastKnownElr(of())
                .setOfflineReplicas(of()))
            .collect(toList());

        return new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic()
            .setName(topicName)
            .setTopicId(definition.topicId())
            .setErrorCode((short) 0)
            .setIsInternal(false)
            .setPartitions(partitions)
            .setTopicAuthorizedOperations(Integer.MIN_VALUE);
    }

    /**
     * Returns the partition count for a topic, consulting the {@link TopicStore} first
     * and falling back to {@code 1} for topics not explicitly created via the admin API.
     *
     * @param topicName the topic name
     * @return the number of partitions
     */
    private int resolvePartitionCount(final String topicName) {
        if (topicStore == null) {
            return 1;
        }
        return topicStore.getTopic(topicName)
            .map(TopicStore.TopicDefinition::numPartitions)
            .orElse(1);
    }

    /**
     * Builds a metadata entry for a topic with the specified number of partitions.
     *
     * @param topicName     the topic name
     * @param numPartitions the number of partitions to advertise
     * @return the topic metadata
     */
    private MetadataResponseData.MetadataResponseTopic buildMetadataTopicResponse(
            final String topicName, final int numPartitions) {

        final var partitions = IntStream.range(0, numPartitions)
            .mapToObj(i -> new MetadataResponseData.MetadataResponsePartition()
                .setPartitionIndex(i)
                .setLeaderId(1)
                .setLeaderEpoch(-1)
                .setReplicaNodes(of(1))
                .setIsrNodes(of(1))
                .setErrorCode((short) 0))
            .collect(toList());

        return new MetadataResponseData.MetadataResponseTopic()
            .setName(topicName)
            .setErrorCode((short) 0)
            .setIsInternal(false)
            .setPartitions(partitions);
    }

    /**
     * Builds a METADATA topic entry carrying {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} with an
     * empty partition list.
     *
     * <p>Used when {@code autoCreateTopicsEnabled} is {@code false} and the requested topic is not
     * registered in the {@link TopicStore}.</p>
     *
     * @param topicName the unknown topic name
     * @return the error topic metadata entry
     */
    private MetadataResponseData.MetadataResponseTopic buildMetadataTopicErrorResponse(
            final String topicName) {
        return new MetadataResponseData.MetadataResponseTopic()
            .setName(topicName)
            .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
            .setIsInternal(false)
            .setPartitions(of());
    }

    /**
     * Creates a METADATA broker entry reflecting the actual server address.
     *
     * @return the configured metadata broker
     */
    private MetadataResponseData.MetadataResponseBroker createMetadataBroker() {
        return new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(1)
            .setHost(getServerHost())
            .setPort(getServerPort())
            .setRack(null);
    }

    /**
     * Creates a DESCRIBE_CLUSTER broker entry reflecting the actual server address.
     *
     * @return the configured describe-cluster broker
     */
    private DescribeClusterResponseData.DescribeClusterBroker createDescribeClusterBroker() {
        return new DescribeClusterResponseData.DescribeClusterBroker()
            .setBrokerId(1)
            .setHost(getServerHost())
            .setPort(getServerPort())
            .setRack(null);
    }

    /**
     * Returns the server port, falling back to {@value DEFAULT_PORT} if unavailable.
     *
     * @return the server port
     */
    private int getServerPort() {
        try {
            return (serverInfo != null) ? serverInfo.getPort() : DEFAULT_PORT;
        } catch (final IOException e) {
            log.warn("Could not get server port, using default {}", DEFAULT_PORT, e);
            return DEFAULT_PORT;
        }
    }

    /**
     * Returns the server host, falling back to {@value DEFAULT_HOST} if unavailable.
     *
     * @return the server host
     */
    private String getServerHost() {
        return (serverInfo != null) ? serverInfo.getHost() : DEFAULT_HOST;
    }
}
