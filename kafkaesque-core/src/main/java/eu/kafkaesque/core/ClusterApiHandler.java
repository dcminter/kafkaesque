package eu.kafkaesque.core;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles Kafka cluster and topology API responses.
 *
 * <p>Covers {@link ApiKeys#API_VERSIONS}, {@link ApiKeys#METADATA},
 * {@link ApiKeys#DESCRIBE_CLUSTER}, and {@link ApiKeys#FIND_COORDINATOR}.</p>
 *
 * @see KafkaProtocolHandler
 */
@Slf4j
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

    @Setter
    private ServerInfo serverInfo;

    /**
     * Creates a new handler with no server info (will use defaults until set).
     */
    ClusterApiHandler() {
    }

    /**
     * Generates an API_VERSIONS response listing all supported Kafka APIs.
     *
     * @param requestHeader the request header
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateApiVersionsResponse(final RequestHeader requestHeader) {
        try {
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

        } catch (final Exception e) {
            log.error("Error generating ApiVersions response", e);
            return null;
        }
    }

    /**
     * Generates a METADATA response for the requested topics.
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateMetadataResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var metadataRequest = new MetadataRequestData(accessor, requestHeader.apiVersion());

            final var data = new MetadataResponseData();

            final var brokers = new MetadataResponseData.MetadataResponseBrokerCollection();
            brokers.add(createMetadataBroker());
            data.setBrokers(brokers);
            data.setClusterId(CLUSTER_ID);
            data.setControllerId(1);

            final var topics = new MetadataResponseData.MetadataResponseTopicCollection();
            final var requestedTopics = metadataRequest.topics();

            if (requestedTopics != null && !requestedTopics.isEmpty()) {
                for (final var requestedTopic : requestedTopics) {
                    final var topicName = requestedTopic.name();
                    if (topicName != null && !topicName.isEmpty()) {
                        topics.add(buildMetadataTopicResponse(topicName));
                    }
                }
            }

            data.setTopics(topics);
            return ResponseSerializer.serialize(requestHeader, data, ApiKeys.METADATA);

        } catch (final Exception e) {
            log.error("Error generating Metadata response", e);
            return null;
        }
    }

    /**
     * Generates a DESCRIBE_CLUSTER response.
     *
     * @param requestHeader the request header
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateDescribeClusterResponse(final RequestHeader requestHeader) {
        try {
            final var brokers = new DescribeClusterResponseData.DescribeClusterBrokerCollection();
            brokers.add(createDescribeClusterBroker());

            final var data = new DescribeClusterResponseData()
                .setClusterId(CLUSTER_ID)
                .setControllerId(1)
                .setBrokers(brokers);

            return ResponseSerializer.serialize(requestHeader, data, ApiKeys.DESCRIBE_CLUSTER);

        } catch (final Exception e) {
            log.error("Error generating DescribeCluster response", e);
            return null;
        }
    }

    /**
     * Generates a FIND_COORDINATOR response returning this broker as the group coordinator.
     *
     * <p>API version 4 and above use a {@code coordinators} array; earlier versions use
     * a single set of top-level fields.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateFindCoordinatorResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new FindCoordinatorRequestData(accessor, requestHeader.apiVersion());

            final var data = new FindCoordinatorResponseData().setThrottleTimeMs(0);

            if (requestHeader.apiVersion() >= 4) {
                final var coordinators = new ArrayList<FindCoordinatorResponseData.Coordinator>();
                for (final var key : request.coordinatorKeys()) {
                    coordinators.add(new FindCoordinatorResponseData.Coordinator()
                        .setKey(key)
                        .setNodeId(1)
                        .setHost(getServerHost())
                        .setPort(getServerPort())
                        .setErrorCode((short) 0));
                }
                data.setCoordinators(coordinators);
            } else {
                data.setErrorCode((short) 0)
                    .setNodeId(1)
                    .setHost(getServerHost())
                    .setPort(getServerPort());
            }

            return ResponseSerializer.serialize(requestHeader, data, ApiKeys.FIND_COORDINATOR);

        } catch (final Exception e) {
            log.error("Error generating FindCoordinator response", e);
            return null;
        }
    }

    /**
     * Generates an {@link Errors#UNSUPPORTED_VERSION} response for APIs that this mock
     * does not support, ensuring the client receives a valid (correctly-correlated)
     * response and does not treat the silence as a protocol error.
     *
     * @param requestHeader the request header (provides correlationId and apiVersion)
     * @param apiKey        the unsupported API key
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateUnsupportedResponse(final RequestHeader requestHeader, final ApiKeys apiKey) {
        try {
            final var errorCode = Errors.UNSUPPORTED_VERSION.code();
            final Message data = switch (apiKey) {
                case GET_TELEMETRY_SUBSCRIPTIONS -> new GetTelemetrySubscriptionsResponseData()
                    .setErrorCode(errorCode);
                case PUSH_TELEMETRY -> new PushTelemetryResponseData()
                    .setErrorCode(errorCode);
                default -> {
                    log.warn("generateUnsupportedResponse called for unexpected API: {}", apiKey);
                    yield null;
                }
            };
            if (data == null) {
                return null;
            }
            return ResponseSerializer.serialize(requestHeader, data, apiKey);
        } catch (final Exception e) {
            log.error("Error generating unsupported response for {}", apiKey, e);
            return null;
        }
    }

    /**
     * Builds the collection of supported API versions, applying version caps where
     * the mock does not support the full range advertised by the Kafka library.
     *
     * @return the collection of supported API version entries
     */
    private ApiVersionsResponseData.ApiVersionCollection buildSupportedApiVersions() {
        final var apiVersions = new ApiVersionsResponseData.ApiVersionCollection();

        for (final var apiKey : ApiKeys.values()) {
            if (apiKey == ApiKeys.CONTROLLED_SHUTDOWN
                    || apiKey == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS
                    || apiKey == ApiKeys.PUSH_TELEMETRY
                    || apiKey.id < 0) {
                continue;
            }
            final short maxVersion = (apiKey == ApiKeys.FETCH)
                ? FETCH_MAX_VERSION
                : apiKey.latestVersion();

            apiVersions.add(new ApiVersionsResponseData.ApiVersion()
                .setApiKey(apiKey.id)
                .setMinVersion(apiKey.oldestVersion())
                .setMaxVersion(maxVersion));
        }

        return apiVersions;
    }

    /**
     * Builds a single-partition metadata entry for a topic.
     *
     * @param topicName the topic name
     * @return the topic metadata
     */
    private MetadataResponseData.MetadataResponseTopic buildMetadataTopicResponse(final String topicName) {
        final var partition = new MetadataResponseData.MetadataResponsePartition()
            .setPartitionIndex(0)
            .setLeaderId(1)
            .setLeaderEpoch(0)
            .setReplicaNodes(List.of(1))
            .setIsrNodes(List.of(1))
            .setErrorCode((short) 0);

        return new MetadataResponseData.MetadataResponseTopic()
            .setName(topicName)
            .setErrorCode((short) 0)
            .setIsInternal(false)
            .setPartitions(List.of(partition));
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
