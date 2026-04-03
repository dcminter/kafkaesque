package eu.kafkaesque.core;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.RequestHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

/**
 * Handles the Kafka wire protocol for client connections.
 *
 * <p>This class is responsible for:</p>
 * <ul>
 *   <li>Parsing incoming Kafka protocol requests</li>
 *   <li>Generating appropriate protocol responses</li>
 *   <li>Managing the read/write lifecycle for client connections</li>
 * </ul>
 *
 * <p>The handler supports the following Kafka APIs:</p>
 * <ul>
 *   <li>{@link ApiKeys#API_VERSIONS} - Returns supported API versions</li>
 *   <li>{@link ApiKeys#METADATA} - Returns cluster and topic metadata</li>
 *   <li>{@link ApiKeys#DESCRIBE_CLUSTER} - Returns cluster information</li>
 *   <li>{@link ApiKeys#FIND_COORDINATOR} - Returns error (not yet implemented)</li>
 * </ul>
 *
 * @see KafkaesqueServer
 * @see ClientConnection
 */
@Slf4j
public final class KafkaProtocolHandler {

    private static final String CLUSTER_ID = "kafkaesque-cluster-001";
    private static final int DEFAULT_PORT = 9092;
    private static final String DEFAULT_HOST = "localhost";

    @Setter
    private ServerInfo serverInfo;

    private final EventStore eventStore;

    /**
     * Creates a new protocol handler with a new event store.
     */
    public KafkaProtocolHandler() {
        this(new EventStore());
    }

    /**
     * Creates a new protocol handler with the specified event store.
     *
     * @param eventStore the event store to use for storing published records
     */
    public KafkaProtocolHandler(final EventStore eventStore) {
        this.eventStore = eventStore;
    }

    /**
     * Gets the event store used by this protocol handler.
     *
     * @return the event store
     */
    public EventStore getEventStore() {
        return eventStore;
    }

    /**
     * Handles incoming data from a client connection.
     *
     * <p>This method reads data from the client, processes complete requests,
     * and registers the connection for writing if responses are generated.</p>
     *
     * @param connection the client connection to read from
     * @param key the selection key for this connection
     * @throws IOException if an I/O error occurs
     */
    public void handleRead(final ClientConnection connection, final SelectionKey key) throws IOException {
        final var buffer = connection.readBuffer();
        final var bytesRead = connection.read();

        if (bytesRead == -1) {
            handleClientDisconnect(connection, key);
            return;
        }

        if (bytesRead > 0) {
            log.debug("Read {} bytes from client", bytesRead);
            processCompleteRequests(connection, buffer);

            // If we have responses to send, register for write
            if (connection.writeBuffer().position() > 0) {
                key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            }
        }
    }

    /**
     * Handles a client disconnection.
     *
     * @param connection the client connection that disconnected
     * @param key the selection key for this connection
     * @throws IOException if an I/O error occurs while closing
     */
    private void handleClientDisconnect(final ClientConnection connection, final SelectionKey key) throws IOException {
        key.cancel();
        connection.channel().close();
        log.debug("Client closed connection");
    }

    /**
     * Processes all complete requests in the buffer.
     *
     * <p>This method reads request size fields and processes complete requests,
     * leaving partial requests in the buffer for the next read cycle.</p>
     *
     * @param connection the client connection
     * @param buffer the buffer containing request data
     */
    private void processCompleteRequests(final ClientConnection connection, final ByteBuffer buffer) {
        buffer.flip();

        while (buffer.remaining() >= 4) {
            buffer.mark();
            final var requestSize = buffer.getInt();

            if (buffer.remaining() < requestSize) {
                // Not enough data for complete request, wait for more
                buffer.reset();
                break;
            }

            // We have a complete request, process it
            processRequest(connection, buffer, requestSize);
        }

        buffer.compact();
    }

    /**
     * Handles writing data to a client connection.
     *
     * <p>This method writes buffered response data to the client socket.</p>
     *
     * @param connection the client connection to write to
     * @param key the selection key for this connection
     * @throws IOException if an I/O error occurs
     */
    public void handleWrite(final ClientConnection connection, final SelectionKey key) throws IOException {
        final var writeBuffer = connection.writeBuffer();

        writeBuffer.flip();
        connection.write();

        if (writeBuffer.hasRemaining()) {
            writeBuffer.compact();
        } else {
            writeBuffer.clear();
            // No more data to write, only listen for reads
            key.interestOps(SelectionKey.OP_READ);
        }
    }

    /**
     * Processes a single complete request from the buffer.
     *
     * @param connection the client connection
     * @param buffer the buffer containing the request
     * @param requestSize the size of the request in bytes
     */
    private void processRequest(final ClientConnection connection, final ByteBuffer buffer, final int requestSize) {
        try {
            final var startPosition = buffer.position();
            log.debug("processRequest: startPosition={}, requestSize={}, buffer.remaining()={}",
                startPosition, requestSize, buffer.remaining());

            // Parse request header to determine API key and version
            final var apiKeyId = buffer.getShort();
            final var apiVersion = buffer.getShort();
            final var correlationId = buffer.getInt();

            // Reset to start of request for proper parsing
            buffer.position(startPosition);

            final var apiKey = ApiKeys.forId(apiKeyId);

            log.debug("Processing request: apiKey={}, apiVersion={}, correlationId={}, size={}",
                     apiKey, apiVersion, correlationId, requestSize);

            // Parse the full request header
            final var header = RequestHeader.parse(buffer);

            // Generate response based on API key
            final var response = switch (apiKey) {
                case API_VERSIONS -> generateApiVersionsResponse(header);
                case METADATA -> generateMetadataResponse(header, buffer);
                case DESCRIBE_CLUSTER -> generateDescribeClusterResponse(header);
                case FIND_COORDINATOR -> generateFindCoordinatorResponse(header, buffer);
                case PRODUCE -> generateProduceResponse(header, buffer);
                default -> {
                    log.warn("Unhandled API key: {} (id={}), returning null response", apiKey, apiKey.id);
                    yield null;
                }
            };

            // Skip to end of request
            final var expectedEndPosition = startPosition + requestSize;
            log.debug("Setting buffer position to {} (start={} + size={}), current position={}",
                expectedEndPosition, startPosition, requestSize, buffer.position());
            buffer.position(expectedEndPosition);

            // Queue response for writing if we have one
            if (response != null) {
                final var writeBuffer = connection.writeBuffer();
                log.debug("Queueing response: size={}, writeBuffer.position()={}",
                    response.remaining(), writeBuffer.position());
                writeBuffer.putInt(response.remaining());
                writeBuffer.put(response);
                log.debug("Response queued, writeBuffer.position()={}", writeBuffer.position());
            } else {
                log.warn("No response generated for request");
            }

        } catch (final Exception e) {
            log.error("Error processing request", e);
        }
    }

    /**
     * Generates an API_VERSIONS response containing all supported Kafka APIs.
     *
     * @param requestHeader the request header
     * @return the response buffer, or null if an error occurs
     */
    private ByteBuffer generateApiVersionsResponse(final RequestHeader requestHeader) {
        try {
            // Create ApiVersions response data
            final var data = new ApiVersionsResponseData()
                .setErrorCode((short) 0)
                .setThrottleTimeMs(0)
                .setApiKeys(buildSupportedApiVersions());

            // Allocate a large buffer (we'll trim it later)
            final var buffer = ByteBuffer.allocate(8192);

            // Write correlation ID as response header
            buffer.putInt(requestHeader.correlationId());

            // Serialize the response data
            final var cache = new ObjectSerializationCache();
            final var accessor = new ByteBufferAccessor(buffer);
            data.write(accessor, cache, requestHeader.apiVersion());

            buffer.flip();
            return buffer;

        } catch (final Exception e) {
            log.error("Error generating ApiVersions response", e);
            return null;
        }
    }

    /**
     * Builds a collection of all supported API versions.
     *
     * <p>This method iterates through all available Kafka API keys and creates
     * API version entries for each, excluding CONTROLLED_SHUTDOWN and any APIs
     * with negative IDs.</p>
     *
     * @return a collection of supported API versions
     */
    private ApiVersionsResponseData.ApiVersionCollection buildSupportedApiVersions() {
        final var apiVersions = new ApiVersionsResponseData.ApiVersionCollection();

        for (final var apiKey : ApiKeys.values()) {
            if (apiKey != ApiKeys.CONTROLLED_SHUTDOWN && apiKey.id >= 0) {
                final var apiVersion = new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(apiKey.id)
                    .setMinVersion(apiKey.oldestVersion())
                    .setMaxVersion(apiKey.latestVersion());
                apiVersions.add(apiVersion);
            }
        }

        return apiVersions;
    }

    /**
     * Generates a METADATA response containing cluster and broker information.
     *
     * <p>This method parses the metadata request to determine which topics are being queried
     * and returns metadata for those topics with a single partition on the broker.</p>
     *
     * @param requestHeader the request header
     * @param buffer the buffer containing the request body
     * @return the response buffer, or null if an error occurs
     */
    private ByteBuffer generateMetadataResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            log.debug("Generating Metadata response for API version: {}", requestHeader.apiVersion());

            // Parse the metadata request to see what topics are requested
            final var accessor = new ByteBufferAccessor(buffer);
            final var metadataRequest = new MetadataRequestData(accessor, requestHeader.apiVersion());

            // Create a minimal metadata response with cluster information
            final var data = new MetadataResponseData();

            // Add broker information with the actual server port
            final var broker = createMetadataBroker();
            final var brokers = new MetadataResponseData.MetadataResponseBrokerCollection();
            brokers.add(broker);

            data.setBrokers(brokers);
            data.setClusterId(CLUSTER_ID);
            data.setControllerId(1);

            // Create topic metadata for requested topics
            final var topics = new MetadataResponseData.MetadataResponseTopicCollection();

            // If specific topics are requested, return metadata for them
            // Note: topics() can be null (meaning all topics) or empty, or contain specific topics
            final var requestedTopics = metadataRequest.topics();
            if (requestedTopics != null && !requestedTopics.isEmpty()) {
                log.debug("Metadata request for {} specific topics", requestedTopics.size());
                for (final var requestedTopic : requestedTopics) {
                    final var topicName = requestedTopic.name();
                    if (topicName != null && !topicName.isEmpty()) {
                        log.debug("Creating metadata for topic: {}", topicName);

                        final var topicMetadata = new MetadataResponseData.MetadataResponseTopic()
                            .setName(topicName)
                            .setErrorCode((short) 0) // No error
                            .setIsInternal(false);

                        // Add a single partition for this topic
                        final var partition = new MetadataResponseData.MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setLeaderId(1)
                            .setLeaderEpoch(0)
                            .setReplicaNodes(java.util.List.of(1))
                            .setIsrNodes(java.util.List.of(1))
                            .setErrorCode((short) 0);

                        topicMetadata.setPartitions(java.util.List.of(partition));
                        topics.add(topicMetadata);
                    }
                }
            } else {
                log.debug("Metadata request for all topics (returning empty list)");
            }

            data.setTopics(topics);

            log.debug("About to serialize response with {} brokers and {} topics",
                data.brokers().size(), data.topics().size());

            return serializeFlexibleResponse(requestHeader, data, ApiKeys.METADATA);

        } catch (final Exception e) {
            log.error("Error generating Metadata response", e);
            return null;
        }
    }

    /**
     * Generates a DESCRIBE_CLUSTER response containing cluster information.
     *
     * @param requestHeader the request header
     * @return the response buffer, or null if an error occurs
     */
    private ByteBuffer generateDescribeClusterResponse(final RequestHeader requestHeader) {
        try {
            log.debug("Generating DescribeCluster response for API version: {}", requestHeader.apiVersion());

            // Create describe cluster response
            final var data = new DescribeClusterResponseData()
                .setClusterId(CLUSTER_ID)
                .setControllerId(1);

            // Add broker information
            final var broker = createDescribeClusterBroker();
            final var brokers = new DescribeClusterResponseData.DescribeClusterBrokerCollection();
            brokers.add(broker);
            data.setBrokers(brokers);

            return serializeFlexibleResponse(requestHeader, data, ApiKeys.DESCRIBE_CLUSTER);

        } catch (final Exception e) {
            log.error("Error generating DescribeCluster response", e);
            return null;
        }
    }

    /**
     * Generates a FIND_COORDINATOR response indicating the coordinator is not available.
     *
     * <p>Consumer-group coordination is not yet implemented; this method returns a
     * properly serialized {@code COORDINATOR_NOT_AVAILABLE} error so the client can
     * parse and handle the response gracefully rather than encountering a schema
     * deserialization failure.</p>
     *
     * <p>API version 4 and above use a {@code coordinators} array keyed by the values
     * from the request; earlier versions use a single top-level error code.</p>
     *
     * @param requestHeader the request header
     * @param buffer the buffer containing the request body, positioned after the header
     * @return the serialized response buffer, or null if an error occurs
     */
    private ByteBuffer generateFindCoordinatorResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new FindCoordinatorRequestData(accessor, requestHeader.apiVersion());

            final var data = new FindCoordinatorResponseData().setThrottleTimeMs(0);

            if (requestHeader.apiVersion() >= 4) {
                // v4+: one Coordinator entry per requested key
                final var coordinators = new java.util.ArrayList<FindCoordinatorResponseData.Coordinator>();
                for (final var key : request.coordinatorKeys()) {
                    coordinators.add(new FindCoordinatorResponseData.Coordinator()
                        .setKey(key)
                        .setNodeId(-1)
                        .setHost("")
                        .setPort(-1)
                        .setErrorCode((short) 15)); // COORDINATOR_NOT_AVAILABLE
                }
                data.setCoordinators(coordinators);
            } else {
                // v0-v3: single top-level error fields
                data.setErrorCode((short) 15) // COORDINATOR_NOT_AVAILABLE
                    .setNodeId(-1)
                    .setHost("")
                    .setPort(-1);
            }

            return serializeFlexibleResponse(requestHeader, data, ApiKeys.FIND_COORDINATOR);

        } catch (final Exception e) {
            log.error("Error generating FindCoordinator response", e);
            return null;
        }
    }

    /**
     * Generates a PRODUCE response after parsing and logging the produce request.
     *
     * <p>This method parses the produce request to extract topics, partitions, and record batches,
     * logs the message content at INFO level, and returns a success response.</p>
     *
     * @param requestHeader the request header
     * @param buffer the buffer containing the request body
     * @return the response buffer, or null if an error occurs
     */
    private ByteBuffer generateProduceResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            log.debug("Generating Produce response for API version: {}", requestHeader.apiVersion());

            // Parse the produce request
            final var accessor = new ByteBufferAccessor(buffer);
            final var produceRequest = new ProduceRequestData(accessor, requestHeader.apiVersion());

            // Log the produce request details
            logProduceRequest(produceRequest);

            // Create produce response with success for all partitions
            final var response = new ProduceResponseData()
                .setThrottleTimeMs(0);

            final var topicResponses = new ProduceResponseData.TopicProduceResponseCollection();

            for (final var topicData : produceRequest.topicData()) {
                final var topicResponse = new ProduceResponseData.TopicProduceResponse()
                    .setName(topicData.name());

                final var partitionResponses = new java.util.ArrayList<ProduceResponseData.PartitionProduceResponse>();

                for (final var partitionData : topicData.partitionData()) {
                    final var partitionResponse = new ProduceResponseData.PartitionProduceResponse()
                        .setIndex(partitionData.index())
                        .setErrorCode((short) 0) // No error
                        .setBaseOffset(0L) // First offset
                        .setLogAppendTimeMs(-1L)
                        .setLogStartOffset(0L);

                    partitionResponses.add(partitionResponse);
                }

                topicResponse.setPartitionResponses(partitionResponses);
                topicResponses.add(topicResponse);
            }

            response.setResponses(topicResponses);

            return serializeFlexibleResponse(requestHeader, response, ApiKeys.PRODUCE);

        } catch (final Exception e) {
            log.error("Error generating Produce response", e);
            return null;
        }
    }

    /**
     * Logs and stores the contents of a produce request.
     *
     * <p>This method extracts and logs topic names, partition IDs, and record details
     * from the produce request, and stores each record in the event store.</p>
     *
     * @param produceRequest the produce request to log and store
     */
    private void logProduceRequest(final ProduceRequestData produceRequest) {
        log.info("Received PRODUCE request: transactionalId={}, acks={}, timeoutMs={}",
            produceRequest.transactionalId(), produceRequest.acks(), produceRequest.timeoutMs());

        for (final var topicData : produceRequest.topicData()) {
            final var topicName = topicData.name();
            log.info("  Topic: {}", topicName);

            for (final var partitionData : topicData.partitionData()) {
                final var partitionIndex = partitionData.index();
                log.info("    Partition: {}", partitionIndex);

                // Parse the records from the partition data
                final var recordsData = partitionData.records();
                if (recordsData instanceof MemoryRecords memoryRecords) {

                    var recordCount = 0;
                    for (final var batch : memoryRecords.batches()) {
                        for (final var record : batch) {
                            recordCount++;

                            final var keySize = record.keySize();
                            final String key = readFromByteBufferToString(record.key(), keySize);

                            final var valueSize = record.valueSize();
                            final String value = readFromByteBufferToString(record.value(), valueSize);

                            // Store the record in the event store
                            final var assignedOffset = eventStore.storeRecord(
                                topicName,
                                partitionIndex,
                                record.timestamp(),
                                key,
                                value
                            );

                            log.info("      Record {}, offset={}, timestamp={}, compressed={}, keySize='{}', key='{}', valueSize='{}', value='{}'",
                                recordCount, assignedOffset, record.timestamp(), record.isCompressed(), keySize, key, valueSize, value);
                        }
                    }

                    log.info("    Total records in partition: {}", recordCount);
                }
            }
        }
    }

    private static String readFromByteBufferToString(final ByteBuffer buffer, final int size) {
        final byte[] raw = new byte[size];
        buffer.get(raw);
        return new String(raw);
    }

    /**
     * Creates a metadata broker with actual server information.
     *
     * @return the configured metadata broker
     */
    private MetadataResponseData.MetadataResponseBroker createMetadataBroker() {
        final var actualPort = getServerPort();
        final var actualHost = getServerHost();

        final var broker = new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(1)
            .setHost(actualHost)
            .setPort(actualPort)
            .setRack(null);  // Use null for rack (nullable field in flexible versions)

        log.debug("Created broker: nodeId={}, host={}, port={}, rack={}",
            broker.nodeId(), broker.host(), broker.port(), broker.rack());

        return broker;
    }

    /**
     * Creates a describe cluster broker with actual server information.
     *
     * @return the configured describe cluster broker
     */
    private DescribeClusterResponseData.DescribeClusterBroker createDescribeClusterBroker() {
        final var actualPort = getServerPort();
        final var actualHost = getServerHost();

        return new DescribeClusterResponseData.DescribeClusterBroker()
            .setBrokerId(1)
            .setHost(actualHost)
            .setPort(actualPort)
            .setRack(null);
    }

    /**
     * Serializes a flexible version response (API version 9+) with proper headers and caching.
     *
     * <p>Flexible versions require pre-calculation of sizes to populate the ObjectSerializationCache
     * before writing the actual data.</p>
     *
     * @param requestHeader the request header
     * @param data the response data to serialize
     * @param apiKey the API key for determining header version
     * @return the serialized response buffer
     */
    private ByteBuffer serializeFlexibleResponse(
            final RequestHeader requestHeader,
            final Message data,
            final ApiKeys apiKey) {

        // Create serialization cache and pre-calculate sizes to populate cache
        final var cache = new ObjectSerializationCache();

        // Write response header for flexible versions (v9+)
        final var responseHeaderData = new ResponseHeaderData()
            .setCorrelationId(requestHeader.correlationId());

        final var headerVersion = apiKey.responseHeaderVersion(requestHeader.apiVersion());

        // Pre-calculate sizes to populate the serialization cache
        final var headerSize = responseHeaderData.size(cache, headerVersion);
        final var bodySize = data.size(cache, requestHeader.apiVersion());

        log.debug("Calculated sizes: header={}, body={}", headerSize, bodySize);

        // Allocate buffer with exact size needed
        final var buffer = ByteBuffer.allocate(headerSize + bodySize);
        final var accessor = new ByteBufferAccessor(buffer);

        log.debug("Writing response header with version={}, correlationId={}",
            headerVersion, requestHeader.correlationId());
        responseHeaderData.write(accessor, cache, headerVersion);

        // Serialize the response data
        log.debug("Writing response body with apiVersion={}", requestHeader.apiVersion());
        data.write(accessor, cache, requestHeader.apiVersion());

        buffer.flip();
        log.debug("Successfully generated response, buffer size: {}", buffer.remaining());
        return buffer;
    }

    /**
     * Gets the server port, with fallback to default.
     *
     * @return the server port, or {@value DEFAULT_PORT} if unavailable
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
     * Gets the server host, with fallback to default.
     *
     * @return the server host, or {@value DEFAULT_HOST} if unavailable
     */
    private String getServerHost() {
        return (serverInfo != null) ? serverInfo.getHost() : DEFAULT_HOST;
    }
}
