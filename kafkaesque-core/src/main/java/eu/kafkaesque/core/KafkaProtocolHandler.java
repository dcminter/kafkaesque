package eu.kafkaesque.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.ApiKeys;
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
 *   <li>Dispatching to the appropriate API handler</li>
 *   <li>Managing the read/write lifecycle for client connections</li>
 * </ul>
 *
 * <p>The handler supports the following Kafka APIs:</p>
 * <ul>
 *   <li>{@link ApiKeys#API_VERSIONS} - Returns supported API versions</li>
 *   <li>{@link ApiKeys#METADATA} - Returns cluster and topic metadata</li>
 *   <li>{@link ApiKeys#DESCRIBE_CLUSTER} - Returns cluster information</li>
 *   <li>{@link ApiKeys#FIND_COORDINATOR} - Returns this broker as the group coordinator</li>
 *   <li>{@link ApiKeys#PRODUCE} - Accepts and stores published records</li>
 *   <li>{@link ApiKeys#JOIN_GROUP} - Handles consumer group join</li>
 *   <li>{@link ApiKeys#SYNC_GROUP} - Stores and returns partition assignments</li>
 *   <li>{@link ApiKeys#HEARTBEAT} - Acknowledges consumer liveness</li>
 *   <li>{@link ApiKeys#LEAVE_GROUP} - Acknowledges consumer departure</li>
 *   <li>{@link ApiKeys#OFFSET_FETCH} - Returns committed offsets for a consumer group</li>
 *   <li>{@link ApiKeys#OFFSET_COMMIT} - Stores committed offsets for a consumer group</li>
 *   <li>{@link ApiKeys#LIST_OFFSETS} - Returns earliest or latest offsets for partitions</li>
 *   <li>{@link ApiKeys#FETCH} - Returns stored records to consumers</li>
 *   <li>{@link ApiKeys#GET_TELEMETRY_SUBSCRIPTIONS} - Returns UNSUPPORTED_VERSION (telemetry not implemented)</li>
 *   <li>{@link ApiKeys#PUSH_TELEMETRY} - Returns UNSUPPORTED_VERSION (telemetry not implemented)</li>
 * </ul>
 *
 * @see KafkaesqueServer
 * @see ClientConnection
 * @see GroupCoordinator
 */
@Slf4j
public final class KafkaProtocolHandler {

    private final EventStore eventStore;
    private final ClusterApiHandler clusterApiHandler;
    private final ConsumerGroupApiHandler consumerGroupApiHandler;
    private final ConsumerDataApiHandler consumerDataApiHandler;
    private final ProducerApiHandler producerApiHandler;

    /**
     * Creates a new protocol handler with a new event store and group coordinator.
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
        final var groupCoordinator = new GroupCoordinator();
        this.clusterApiHandler = new ClusterApiHandler();
        this.consumerGroupApiHandler = new ConsumerGroupApiHandler(groupCoordinator);
        this.consumerDataApiHandler = new ConsumerDataApiHandler(eventStore, groupCoordinator);
        this.producerApiHandler = new ProducerApiHandler(eventStore);
    }

    /**
     * Sets the server info used to advertise the broker's host and port in cluster responses.
     *
     * @param serverInfo the server info to use
     */
    public void setServerInfo(final ServerInfo serverInfo) {
        clusterApiHandler.setServerInfo(serverInfo);
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
     * @param key        the selection key for this connection
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

            if (connection.writeBuffer().position() > 0) {
                key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            }
        }
    }

    /**
     * Handles writing buffered response data to a client connection.
     *
     * @param connection the client connection to write to
     * @param key        the selection key for this connection
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
            key.interestOps(SelectionKey.OP_READ);
        }
    }

    /**
     * Handles a client disconnection.
     *
     * @param connection the client connection that disconnected
     * @param key        the selection key for this connection
     * @throws IOException if an I/O error occurs while closing
     */
    private void handleClientDisconnect(final ClientConnection connection, final SelectionKey key) throws IOException {
        key.cancel();
        connection.channel().close();
        log.debug("Client closed connection");
    }

    /**
     * Processes all complete requests in the buffer, leaving any partial request in place.
     *
     * @param connection the client connection
     * @param buffer     the buffer containing request data
     */
    private void processCompleteRequests(final ClientConnection connection, final ByteBuffer buffer) {
        buffer.flip();

        while (buffer.remaining() >= 4) {
            buffer.mark();
            final var requestSize = buffer.getInt();

            if (buffer.remaining() < requestSize) {
                buffer.reset();
                break;
            }

            processRequest(connection, buffer, requestSize);
        }

        buffer.compact();
    }

    /**
     * Processes a single complete request from the buffer.
     *
     * @param connection  the client connection
     * @param buffer      the buffer positioned at the start of the request
     * @param requestSize the byte length of the request
     */
    private void processRequest(final ClientConnection connection, final ByteBuffer buffer, final int requestSize) {
        try {
            final var startPosition = buffer.position();

            final var apiKeyId = buffer.getShort();
            final var apiVersion = buffer.getShort();
            final var correlationId = buffer.getInt();

            buffer.position(startPosition);

            final var apiKey = ApiKeys.forId(apiKeyId);

            log.debug("Processing request: apiKey={}, apiVersion={}, correlationId={}, size={}",
                apiKey, apiVersion, correlationId, requestSize);

            final var header = RequestHeader.parse(buffer);

            final var response = switch (apiKey) {
                case API_VERSIONS     -> clusterApiHandler.generateApiVersionsResponse(header);
                case METADATA         -> clusterApiHandler.generateMetadataResponse(header, buffer);
                case DESCRIBE_CLUSTER -> clusterApiHandler.generateDescribeClusterResponse(header);
                case FIND_COORDINATOR -> clusterApiHandler.generateFindCoordinatorResponse(header, buffer);
                case PRODUCE          -> producerApiHandler.generateProduceResponse(header, buffer);
                case JOIN_GROUP       -> consumerGroupApiHandler.generateJoinGroupResponse(header, buffer);
                case SYNC_GROUP       -> consumerGroupApiHandler.generateSyncGroupResponse(header, buffer);
                case HEARTBEAT        -> consumerGroupApiHandler.generateHeartbeatResponse(header, buffer);
                case LEAVE_GROUP      -> consumerGroupApiHandler.generateLeaveGroupResponse(header, buffer);
                case OFFSET_FETCH     -> consumerDataApiHandler.generateOffsetFetchResponse(header, buffer);
                case OFFSET_COMMIT    -> consumerDataApiHandler.generateOffsetCommitResponse(header, buffer);
                case LIST_OFFSETS     -> consumerDataApiHandler.generateListOffsetsResponse(header, buffer);
                case FETCH            -> consumerDataApiHandler.generateFetchResponse(header, buffer);
                case GET_TELEMETRY_SUBSCRIPTIONS, PUSH_TELEMETRY -> {
                    log.debug("Telemetry API not supported: {}", apiKey);
                    yield clusterApiHandler.generateUnsupportedResponse(header, apiKey);
                }
                default -> {
                    log.warn("Unhandled API key: {} (id={})", apiKey, apiKey.id);
                    yield null;
                }
            };

            buffer.position(startPosition + requestSize);

            if (response != null) {
                final var writeBuffer = connection.writeBuffer();
                writeBuffer.putInt(response.remaining());
                writeBuffer.put(response);
            }

        } catch (final Exception e) {
            log.error("Error processing request", e);
        }
    }
}
