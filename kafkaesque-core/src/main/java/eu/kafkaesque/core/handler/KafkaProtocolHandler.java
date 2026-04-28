package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.ServerInfo;
import eu.kafkaesque.core.connection.ClientConnection;
import eu.kafkaesque.core.listener.ListenerRegistry;
import eu.kafkaesque.core.storage.AclStore;
import eu.kafkaesque.core.storage.EventStore;
import eu.kafkaesque.core.storage.TopicStore;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Handles the Kafka wire protocol for client connections.
 *
 * <p>This class is responsible for:</p>
 * <ul>
 *   <li>Parsing incoming Kafka protocol requests</li>
 *   <li>Dispatching to the appropriate API handler</li>
 *   <li>Managing the read/write lifecycle for client connections</li>
 *   <li>Draining deferred responses (e.g. deferred JoinGroup responses) that are enqueued
 *       by background threads and must be flushed on the NIO event loop thread</li>
 * </ul>
 *
 * <p>The handler supports the following Kafka APIs:</p>
 * <ul>
 *   <li>{@link ApiKeys#API_VERSIONS} - Returns supported API versions</li>
 *   <li>{@link ApiKeys#METADATA} - Returns cluster and topic metadata</li>
 *   <li>{@link ApiKeys#DESCRIBE_CLUSTER} - Returns cluster information</li>
 *   <li>{@link ApiKeys#FIND_COORDINATOR} - Returns this broker as the group/transaction coordinator</li>
 *   <li>{@link ApiKeys#PRODUCE} - Accepts and stores published records</li>
 *   <li>{@link ApiKeys#JOIN_GROUP} - Handles consumer group join (response is deferred)</li>
 *   <li>{@link ApiKeys#SYNC_GROUP} - Stores and returns partition assignments</li>
 *   <li>{@link ApiKeys#HEARTBEAT} - Acknowledges consumer liveness</li>
 *   <li>{@link ApiKeys#LEAVE_GROUP} - Acknowledges consumer departure</li>
 *   <li>{@link ApiKeys#OFFSET_FETCH} - Returns committed offsets for a consumer group</li>
 *   <li>{@link ApiKeys#OFFSET_COMMIT} - Stores committed offsets for a consumer group</li>
 *   <li>{@link ApiKeys#LIST_OFFSETS} - Returns earliest or latest offsets for partitions</li>
 *   <li>{@link ApiKeys#FETCH} - Returns stored records to consumers (honours isolation level)</li>
 *   <li>{@link ApiKeys#CREATE_TOPICS} - Registers topics in the topic store</li>
 *   <li>{@link ApiKeys#DESCRIBE_TOPIC_PARTITIONS} - Returns partition details for registered topics</li>
 *   <li>{@link ApiKeys#INCREMENTAL_ALTER_CONFIGS} - Accepts broker/topic config changes (no-op: policies applied at fetch time)</li>
 *   <li>{@link ApiKeys#INIT_PRODUCER_ID} - Assigns a stable producer ID and epoch for transactions</li>
 *   <li>{@link ApiKeys#ADD_PARTITIONS_TO_TXN} - Registers partition involvement in a transaction</li>
 *   <li>{@link ApiKeys#ADD_OFFSETS_TO_TXN} - Registers consumer-group involvement in a transaction</li>
 *   <li>{@link ApiKeys#END_TXN} - Commits or aborts an open transaction</li>
 *   <li>{@link ApiKeys#WRITE_TXN_MARKERS} - Broker-internal marker writing (returns success)</li>
 *   <li>{@link ApiKeys#TXN_OFFSET_COMMIT} - Commits offsets transactionally</li>
 *   <li>{@link ApiKeys#CREATE_ACLS} - Creates ACL bindings in the store</li>
 *   <li>{@link ApiKeys#DESCRIBE_ACLS} - Returns ACL bindings matching the request filter</li>
 *   <li>{@link ApiKeys#DELETE_ACLS} - Deletes ACL bindings matching the request filters</li>
 *   <li>{@link ApiKeys#GET_TELEMETRY_SUBSCRIPTIONS} - Returns UNSUPPORTED_VERSION (telemetry not implemented)</li>
 *   <li>{@link ApiKeys#PUSH_TELEMETRY} - Returns UNSUPPORTED_VERSION (telemetry not implemented)</li>
 * </ul>
 *
 * @see ClientConnection
 * @see GroupCoordinator
 */
@Slf4j
public final class KafkaProtocolHandler {

    @Getter
    private final ListenerRegistry listenerRegistry;
    @Getter
    private final EventStore eventStore;
    @Getter
    private final TopicStore topicStore;
    private final ClusterApiHandler clusterApiHandler;
    private final ConsumerGroupApiHandler consumerGroupApiHandler;
    private final ConsumerDataApiHandler consumerDataApiHandler;
    private final ProducerApiHandler producerApiHandler;
    private final AdminApiHandler adminApiHandler;
    private final TransactionApiHandler transactionApiHandler;
    private final AclApiHandler aclApiHandler;

    /**
     * Maps each supported {@link ApiKeys} value to the {@link ApiRequestHandler} that produces
     * its response. Built once in the constructor, exposed as an unmodifiable view, and read
     * by {@link #dispatchRequest}.
     */
    private final Map<ApiKeys, ApiRequestHandler> handlers;

    /**
     * Queue of responses that could not be written immediately (e.g. deferred JoinGroup
     * responses) and must be dispatched on the NIO event loop thread via
     * {@link #drainDeferredResponses()}.
     */
    private final ConcurrentLinkedQueue<DeferredResponse> pendingResponses = new ConcurrentLinkedQueue<>();

    /**
     * The NIO selector for the server event loop; used to wake the selector when a deferred
     * response is enqueued so it is delivered promptly. Set via {@link #setSelector(Selector)}.
     */
    private final AtomicReference<Selector> selector = new AtomicReference<>();

    /**
     * Bundles a {@link ListenerRegistry} with the {@link EventStore} it was given to,
     * so that a single {@code this(...)} call can forward both from a static factory.
     */
    private static final class SharedStores {

        /** The event store. */
        private final EventStore eventStore;

        /** The shared listener registry. */
        private final ListenerRegistry listenerRegistry;

        /**
         * Creates a new {@code SharedStores} bundle.
         *
         * @param eventStore       the event store
         * @param listenerRegistry the shared listener registry
         */
        SharedStores(final EventStore eventStore, final ListenerRegistry listenerRegistry) {
            this.eventStore = eventStore;
            this.listenerRegistry = listenerRegistry;
        }

        /**
         * Returns the event store.
         *
         * @return the event store
         */
        EventStore eventStore() {
            return eventStore;
        }

        /**
         * Returns the shared listener registry.
         *
         * @return the shared listener registry
         */
        ListenerRegistry listenerRegistry() {
            return listenerRegistry;
        }
    }

    /**
     * Creates a new protocol handler with the given server info and a fresh event store,
     * with auto-topic-creation enabled.
     *
     * @param serverInfo the server info used to advertise host and port in cluster responses;
     *                   may be {@code null} to use built-in defaults
     */
    public KafkaProtocolHandler(final ServerInfo serverInfo) {
        this(serverInfo, true);
    }

    /**
     * Creates a new protocol handler with the given server info, a fresh event store,
     * and configurable auto-topic-creation behaviour.
     *
     * <p>A single {@link ListenerRegistry} is created and shared between the new
     * {@link EventStore} and {@link TopicStore}.</p>
     *
     * @param serverInfo              the server info used to advertise host and port in cluster responses;
     *                                may be {@code null} to use built-in defaults
     * @param autoCreateTopicsEnabled {@code false} to return {@code UNKNOWN_TOPIC_OR_PARTITION}
     *                                for unknown topics instead of auto-creating them
     */
    public KafkaProtocolHandler(final ServerInfo serverInfo, final boolean autoCreateTopicsEnabled) {
        this(serverInfo, createSharedStores(), autoCreateTopicsEnabled);
    }

    /**
     * Creates a new protocol handler with a fresh event store and no server info,
     * with auto-topic-creation enabled.
     */
    public KafkaProtocolHandler() {
        this(null, true);
    }

    /**
     * Delegating constructor used by {@link #KafkaProtocolHandler(ServerInfo, boolean)} to
     * forward the bundled stores created by {@link #createSharedStores()}.
     *
     * @param serverInfo              the server info for cluster responses
     * @param stores                  the bundled event store and shared listener registry
     * @param autoCreateTopicsEnabled whether to auto-create topics
     */
    private KafkaProtocolHandler(
            final ServerInfo serverInfo,
            final SharedStores stores,
            final boolean autoCreateTopicsEnabled) {
        this(serverInfo, stores.eventStore(), stores.listenerRegistry(), autoCreateTopicsEnabled);
    }

    /**
     * Primary constructor that wires all components together with a shared listener registry.
     *
     * @param serverInfo              the server info used to advertise host and port in cluster responses;
     *                                may be {@code null} to use built-in defaults
     * @param eventStore              the event store to use for storing published records
     * @param listenerRegistry        the listener registry shared across stores
     * @param autoCreateTopicsEnabled {@code false} to return {@code UNKNOWN_TOPIC_OR_PARTITION}
     *                                for unknown topics instead of auto-creating them
     */
    public KafkaProtocolHandler(
            final ServerInfo serverInfo,
            final EventStore eventStore,
            final ListenerRegistry listenerRegistry,
            final boolean autoCreateTopicsEnabled) {
        this.listenerRegistry = listenerRegistry;
        this.eventStore = eventStore;
        final var groupCoordinator = new GroupCoordinator();
        final var transactionCoordinator = new TransactionCoordinator(eventStore);
        final var fetchSessionCoordinator = new FetchSessionCoordinator();
        this.topicStore = new TopicStore(listenerRegistry);
        this.clusterApiHandler = new ClusterApiHandler(serverInfo, this.topicStore, autoCreateTopicsEnabled);
        this.consumerGroupApiHandler = new ConsumerGroupApiHandler(groupCoordinator, this::enqueueResponse);
        this.consumerDataApiHandler = new ConsumerDataApiHandler(
            eventStore, groupCoordinator, this.topicStore, fetchSessionCoordinator);
        this.producerApiHandler = new ProducerApiHandler(eventStore, this.topicStore, autoCreateTopicsEnabled);
        this.adminApiHandler = new AdminApiHandler(this.topicStore, eventStore);
        this.transactionApiHandler = new TransactionApiHandler(transactionCoordinator, groupCoordinator);
        this.aclApiHandler = new AclApiHandler(new AclStore());
        this.handlers = buildHandlers();
    }

    /**
     * Creates a shared {@link ListenerRegistry} and an {@link EventStore} wired to it.
     *
     * @return a bundle containing both the event store and the shared registry
     */
    private static SharedStores createSharedStores() {
        final var registry = new ListenerRegistry();
        return new SharedStores(new EventStore(registry), registry);
    }

    /**
     * Sets the NIO selector for the server event loop, allowing deferred responses
     * to wake the selector when they are enqueued.
     *
     * @param selector the NIO selector to use for wakeup notifications
     */
    public void setSelector(final Selector selector) {
        this.selector.set(selector);
    }

    /**
     * Pre-registers a topic with the given configuration and compression.
     *
     * @param name              the topic name
     * @param numPartitions     the number of partitions
     * @param replicationFactor the replication factor
     * @param compression       the compression to apply when serving FetchResponses for this topic
     */
    public void createTopic(
            final String name, final int numPartitions,
            final short replicationFactor, final Compression compression) {
        topicStore.createTopic(name, numPartitions, replicationFactor, compression);
    }

    /**
     * Drains all pending deferred responses, writing each response to its connection's
     * write buffer and registering {@code OP_WRITE} on the corresponding selection key.
     *
     * <p>Must be called from the NIO event loop thread on each loop iteration so that
     * deferred responses (e.g. JoinGroup responses held until the rebalance window
     * closes) are delivered promptly.</p>
     */
    public void drainDeferredResponses() {
        DeferredResponse deferred;
        while ((deferred = pendingResponses.poll()) != null) {
            if (deferred.key().isValid()) {
                writeResponse(deferred.connection(), deferred.response());
                deferred.key().interestOps(deferred.key().interestOps() | SelectionKey.OP_WRITE);
            }
        }
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
            processCompleteRequests(connection, buffer, key);

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
     * Shuts down background resources owned by this handler (e.g. the rebalance scheduler
     * and the listener consumer thread).
     */
    public void close() {
        consumerGroupApiHandler.close();
        listenerRegistry.close();
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
     * @param key        the selection key for this connection
     */
    private void processCompleteRequests(
            final ClientConnection connection,
            final ByteBuffer buffer,
            final SelectionKey key) {
        buffer.flip();

        while (buffer.remaining() >= 4) {
            buffer.mark();
            final var requestSize = buffer.getInt();

            if (buffer.remaining() < requestSize) {
                buffer.reset();
                break;
            }

            processRequest(connection, buffer, requestSize, key);
        }

        buffer.compact();
    }

    /**
     * Processes a single complete request from the buffer.
     *
     * @param connection  the client connection
     * @param buffer      the buffer positioned at the start of the request
     * @param requestSize the byte length of the request
     * @param key         the selection key for this connection
     */
    private void processRequest(
            final ClientConnection connection,
            final ByteBuffer buffer,
            final int requestSize,
            final SelectionKey key) {
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
            final var response = dispatchRequest(apiKey, header, buffer, connection, key);
            buffer.position(startPosition + requestSize);
            writeResponse(connection, response);
        } catch (final Throwable e) {
            log.error("Error processing request", e);
        }
    }

    /**
     * Routes a parsed request to its registered {@link ApiRequestHandler} via the dispatcher
     * map. Returns {@code null} for unhandled keys and for handlers that defer their response.
     *
     * @param apiKey     the API key identifying the request type
     * @param header     the parsed request header
     * @param buffer     the request body buffer
     * @param connection the client connection
     * @param key        the selection key for this connection
     * @return the serialised response buffer, or {@code null} if no response should be written
     */
    private ByteBuffer dispatchRequest(
            final ApiKeys apiKey,
            final RequestHeader header,
            final ByteBuffer buffer,
            final ClientConnection connection,
            final SelectionKey key) {
        final var handler = handlers.get(apiKey);
        if (handler == null) {
            log.warn("Unhandled API key: {} (id={})", apiKey, apiKey.id);
            return null;
        }
        return handler.handle(new RequestContext(apiKey, header, buffer, connection, key));
    }

    /**
     * Builds the dispatcher map from {@link ApiKeys} to {@link ApiRequestHandler}, registering
     * one entry per supported request type. Called from the primary constructor after all
     * collaborators are wired.
     *
     * @return an immutable view of the populated dispatcher map
     */
    private Map<ApiKeys, ApiRequestHandler> buildHandlers() {
        final var map = new EnumMap<ApiKeys, ApiRequestHandler>(ApiKeys.class);
        registerClusterHandlers(map);
        registerProducerHandlers(map);
        registerConsumerGroupHandlers(map);
        registerConsumerDataHandlers(map);
        registerAdminHandlers(map);
        registerAclHandlers(map);
        registerTransactionHandlers(map);
        registerTelemetryHandlers(map);
        return Collections.unmodifiableMap(map);
    }

    /**
     * Registers the cluster-related API handlers (versions, metadata, coordinator lookup,
     * cluster description, and topic-partition description).
     *
     * @param map the mutable dispatcher map to populate
     */
    private void registerClusterHandlers(final Map<ApiKeys, ApiRequestHandler> map) {
        map.put(ApiKeys.API_VERSIONS,
            ctx -> clusterApiHandler.generateApiVersionsResponse(ctx.header()));
        map.put(ApiKeys.METADATA,
            ctx -> clusterApiHandler.generateMetadataResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.DESCRIBE_CLUSTER,
            ctx -> clusterApiHandler.generateDescribeClusterResponse(ctx.header()));
        map.put(ApiKeys.FIND_COORDINATOR,
            ctx -> clusterApiHandler.generateFindCoordinatorResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.DESCRIBE_TOPIC_PARTITIONS,
            ctx -> clusterApiHandler.generateDescribeTopicPartitionsResponse(ctx.header(), ctx.buffer()));
    }

    /**
     * Registers the producer API handler ({@code PRODUCE}).
     *
     * @param map the mutable dispatcher map to populate
     */
    private void registerProducerHandlers(final Map<ApiKeys, ApiRequestHandler> map) {
        map.put(ApiKeys.PRODUCE,
            ctx -> producerApiHandler.generateProduceResponse(ctx.header(), ctx.buffer()));
    }

    /**
     * Registers the consumer-group lifecycle handlers. {@code JOIN_GROUP} and
     * {@code SYNC_GROUP} pass the connection and selection key from the context so that
     * deferred responses can be written when the rebalance window closes.
     *
     * @param map the mutable dispatcher map to populate
     */
    private void registerConsumerGroupHandlers(final Map<ApiKeys, ApiRequestHandler> map) {
        map.put(ApiKeys.JOIN_GROUP,
            ctx -> consumerGroupApiHandler.generateJoinGroupResponse(
                ctx.header(), ctx.buffer(), ctx.connection(), ctx.key()));
        map.put(ApiKeys.SYNC_GROUP,
            ctx -> consumerGroupApiHandler.generateSyncGroupResponse(
                ctx.header(), ctx.buffer(), ctx.connection(), ctx.key()));
        map.put(ApiKeys.HEARTBEAT,
            ctx -> consumerGroupApiHandler.generateHeartbeatResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.LEAVE_GROUP,
            ctx -> consumerGroupApiHandler.generateLeaveGroupResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.LIST_GROUPS,
            ctx -> consumerGroupApiHandler.generateListGroupsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.CONSUMER_GROUP_DESCRIBE,
            ctx -> consumerGroupApiHandler.generateConsumerGroupDescribeResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.DELETE_GROUPS,
            ctx -> consumerGroupApiHandler.generateDeleteGroupsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.DESCRIBE_GROUPS,
            ctx -> consumerGroupApiHandler.generateDescribeGroupsResponse(ctx.header(), ctx.buffer()));
    }

    /**
     * Registers the consumer-data API handlers (offsets and fetches).
     *
     * @param map the mutable dispatcher map to populate
     */
    private void registerConsumerDataHandlers(final Map<ApiKeys, ApiRequestHandler> map) {
        map.put(ApiKeys.OFFSET_FETCH,
            ctx -> consumerDataApiHandler.generateOffsetFetchResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.OFFSET_COMMIT,
            ctx -> consumerDataApiHandler.generateOffsetCommitResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.LIST_OFFSETS,
            ctx -> consumerDataApiHandler.generateListOffsetsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.FETCH,
            ctx -> consumerDataApiHandler.generateFetchResponse(ctx.header(), ctx.buffer()));
    }

    /**
     * Registers the admin API handlers (topic and config administration).
     *
     * @param map the mutable dispatcher map to populate
     */
    private void registerAdminHandlers(final Map<ApiKeys, ApiRequestHandler> map) {
        map.put(ApiKeys.CREATE_TOPICS,
            ctx -> adminApiHandler.generateCreateTopicsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.DELETE_TOPICS,
            ctx -> adminApiHandler.generateDeleteTopicsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.DESCRIBE_CONFIGS,
            ctx -> adminApiHandler.generateDescribeConfigsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.ALTER_CONFIGS,
            ctx -> adminApiHandler.generateAlterConfigsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.CREATE_PARTITIONS,
            ctx -> adminApiHandler.generateCreatePartitionsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.DELETE_RECORDS,
            ctx -> adminApiHandler.generateDeleteRecordsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.INCREMENTAL_ALTER_CONFIGS,
            ctx -> adminApiHandler.generateIncrementalAlterConfigsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.ELECT_LEADERS,
            ctx -> adminApiHandler.generateElectLeadersResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.DESCRIBE_LOG_DIRS,
            ctx -> adminApiHandler.generateDescribeLogDirsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.ALTER_REPLICA_LOG_DIRS,
            ctx -> adminApiHandler.generateAlterReplicaLogDirsResponse(ctx.header(), ctx.buffer()));
    }

    /**
     * Registers the ACL API handlers.
     *
     * @param map the mutable dispatcher map to populate
     */
    private void registerAclHandlers(final Map<ApiKeys, ApiRequestHandler> map) {
        map.put(ApiKeys.CREATE_ACLS,
            ctx -> aclApiHandler.generateCreateAclsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.DESCRIBE_ACLS,
            ctx -> aclApiHandler.generateDescribeAclsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.DELETE_ACLS,
            ctx -> aclApiHandler.generateDeleteAclsResponse(ctx.header(), ctx.buffer()));
    }

    /**
     * Registers the transaction API handlers.
     *
     * @param map the mutable dispatcher map to populate
     */
    private void registerTransactionHandlers(final Map<ApiKeys, ApiRequestHandler> map) {
        map.put(ApiKeys.INIT_PRODUCER_ID,
            ctx -> transactionApiHandler.generateInitProducerIdResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.ADD_PARTITIONS_TO_TXN,
            ctx -> transactionApiHandler.generateAddPartitionsToTxnResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.ADD_OFFSETS_TO_TXN,
            ctx -> transactionApiHandler.generateAddOffsetsToTxnResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.END_TXN,
            ctx -> transactionApiHandler.generateEndTxnResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.WRITE_TXN_MARKERS,
            ctx -> transactionApiHandler.generateWriteTxnMarkersResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.TXN_OFFSET_COMMIT,
            ctx -> transactionApiHandler.generateTxnOffsetCommitResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.LIST_TRANSACTIONS,
            ctx -> transactionApiHandler.generateListTransactionsResponse(ctx.header(), ctx.buffer()));
        map.put(ApiKeys.DESCRIBE_TRANSACTIONS,
            ctx -> transactionApiHandler.generateDescribeTransactionsResponse(ctx.header(), ctx.buffer()));
    }

    /**
     * Registers the telemetry API handlers, which return an {@code UNSUPPORTED_VERSION} response
     * because telemetry collection is not implemented.
     *
     * @param map the mutable dispatcher map to populate
     */
    private void registerTelemetryHandlers(final Map<ApiKeys, ApiRequestHandler> map) {
        final ApiRequestHandler telemetry = ctx -> {
            log.debug("Telemetry API not supported: {}", ctx.apiKey());
            return clusterApiHandler.generateUnsupportedResponse(ctx.header(), ctx.apiKey());
        };
        map.put(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, telemetry);
        map.put(ApiKeys.PUSH_TELEMETRY, telemetry);
    }

    /**
     * Returns the set of {@link ApiKeys} values for which a handler is registered.
     *
     * <p>Exposed at package-private visibility so that unit tests can assert the supported set
     * without resorting to reflection. The returned set is the live key set of an unmodifiable
     * map; callers must not attempt to mutate it.</p>
     *
     * @return an unmodifiable view of the registered API keys
     */
    Set<ApiKeys> registeredApiKeys() {
        return handlers.keySet();
    }

    /**
     * Enqueues a deferred response for delivery on the next event loop iteration and
     * wakes the selector so the response is not held until the next select timeout.
     *
     * @param deferred the deferred response to enqueue
     */
    private void enqueueResponse(final DeferredResponse deferred) {
        pendingResponses.offer(deferred);
        final var sel = selector.get();
        if (sel != null) {
            sel.wakeup();
        }
    }

    /**
     * Writes a serialised response buffer to the client's write buffer, prefixed with its length.
     *
     * @param connection the client connection to write to
     * @param response   the serialised response buffer, or {@code null} if there is nothing to send
     */
    private static void writeResponse(final ClientConnection connection, final ByteBuffer response) {
        if (response != null) {
            final var writeBuffer = connection.writeBuffer();
            writeBuffer.putInt(response.remaining());
            writeBuffer.put(response);
        }
    }
}
