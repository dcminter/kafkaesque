package eu.kafkaesque.core;

import eu.kafkaesque.core.connection.ClientConnection;
import eu.kafkaesque.core.handler.KafkaProtocolHandler;
import eu.kafkaesque.core.storage.StoredRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.compress.Compression;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main Kafkaesque server that implements the Kafka wire protocol.
 *
 * <p>This server listens on a socket and handles client connections using non-blocking I/O (NIO).
 * It implements the Kafka wire protocol directly without running an actual Kafka broker instance.</p>
 *
 * <p>The server runs in its own daemon thread and uses a selector-based event loop to handle
 * multiple client connections efficiently.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * try (var server = new KafkaesqueServer("localhost", 0)) {
 *     server.start();
 *     String bootstrapServers = server.getBootstrapServers();
 *     // Use bootstrapServers with Kafka clients
 * }
 * }</pre>
 *
 * @see ServerInfo
 * @see ClientConnection
 * @see KafkaProtocolHandler
 */
@Slf4j
public final class KafkaesqueServer implements AutoCloseable, ServerInfo {

    private final String host;
    private final int port;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final KafkaProtocolHandler protocolHandler;

    private ServerSocketChannel serverChannel;
    private Selector selector;
    private Thread serverThread;

    /**
     * Creates a new Kafkaesque server that will listen on the specified host and port.
     *
     * <p>The server is not started automatically; call {@link #start()} to begin accepting connections.</p>
     *
     * @param host the host address to bind to (e.g., "localhost" or "0.0.0.0")
     * @param port the port to bind to, or 0 to use an ephemeral port
     */
    public KafkaesqueServer(final String host, final int port) {
        this.host = host;
        this.port = port;
        this.protocolHandler = new KafkaProtocolHandler(this);
    }

    /**
     * Starts the Kafkaesque server and begins accepting client connections.
     *
     * <p>This method opens the server socket, binds it to the configured host and port,
     * and starts a background thread to handle the event loop.</p>
     *
     * @throws IOException if the server cannot be started (e.g., port already in use)
     * @throws IllegalStateException if the server is already running
     */
    public void start() throws IOException {
        if (running.get()) {
            throw new IllegalStateException("Server is already running");
        }

        serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(host, port));
        serverChannel.configureBlocking(false);

        selector = Selector.open();
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        running.set(true);

        serverThread = Thread.ofPlatform()
            .name("kafkaesque-server")
            .daemon(false)
            .start(this::runEventLoop);

        log.info("Kafkaesque server started on {}:{}", host, port);
    }

    /**
     * Gets the actual port the server is listening on.
     *
     * <p>This is particularly useful when binding to port 0 (ephemeral port),
     * as the operating system will assign an available port.</p>
     *
     * @return the port number the server is bound to
     * @throws IOException if the port cannot be determined
     * @throws IllegalStateException if the server has not been started
     */
    @Override
    public int getPort() throws IOException {
        if (serverChannel == null) {
            throw new IllegalStateException("Server not started");
        }
        return switch (serverChannel.getLocalAddress()) {
            case InetSocketAddress addr -> addr.getPort();
            case null, default -> throw new IOException("Cannot determine port");
        };
    }

    /**
     * Gets the host the server is listening on.
     *
     * @return the host address
     */
    @Override
    public String getHost() {
        return host;
    }

    /**
     * Gets the bootstrap servers string for Kafka clients.
     *
     * <p>This returns a string in the format "host:port" that can be used as the
     * {@code bootstrap.servers} configuration for Kafka producers and consumers.</p>
     *
     * @return the bootstrap servers string (e.g., "localhost:9092")
     * @throws IOException if the port cannot be determined
     */
    public String getBootstrapServers() throws IOException {
        return "%s:%d".formatted(host, getPort());
    }

    /**
     * Main event loop that processes client connections and I/O operations.
     *
     * <p>This method runs in a separate thread and continuously processes selector events
     * until the server is stopped.</p>
     */
    private void runEventLoop() {
        log.debug("Server event loop started");

        while (running.get()) {
            try {
                selector.select(1000);

                if (!running.get() || !selector.isOpen()) {
                    break;
                }

                final var selectedKeys = selector.selectedKeys();
                for (final var iterator = selectedKeys.iterator(); iterator.hasNext();) {
                    final var key = iterator.next();
                    iterator.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isAcceptable()) {
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    } else if (key.isWritable()) {
                        handleWrite(key);
                    }
                }
            } catch (final ClosedSelectorException e) {
                break;
            } catch (final IOException e) {
                if (running.get()) {
                    log.error("Error in server event loop", e);
                }
            }
        }

        log.debug("Server event loop stopped");
    }

    /**
     * Handles accepting a new client connection.
     *
     * @param key the selection key representing the acceptable server socket
     * @throws IOException if an I/O error occurs
     */
    private void handleAccept(final SelectionKey key) throws IOException {
        final var serverChannel = (ServerSocketChannel) key.channel();
        final var clientChannel = serverChannel.accept();

        if (clientChannel != null) {
            clientChannel.configureBlocking(false);

            if (!selector.isOpen()) {
                clientChannel.close();
                return;
            }

            final var clientKey = clientChannel.register(selector, SelectionKey.OP_READ);

            final var connection = new ClientConnection(clientChannel);
            clientKey.attach(connection);

            log.debug("Accepted new client connection from {}", clientChannel.getRemoteAddress());
        }
    }

    /**
     * Handles reading data from a client connection.
     *
     * @param key the selection key representing the readable client socket
     */
    private void handleRead(final SelectionKey key) {
        if (key.attachment() instanceof final ClientConnection connection) {
            try {
                protocolHandler.handleRead(connection, key);
            } catch (final IOException e) {
                log.error("Error handling read from client", e);
                closeConnection(key);
            }
        }
    }

    /**
     * Handles writing data to a client connection.
     *
     * @param key the selection key representing the writable client socket
     */
    private void handleWrite(final SelectionKey key) {
        if (key.attachment() instanceof final ClientConnection connection) {
            try {
                protocolHandler.handleWrite(connection, key);
            } catch (final IOException e) {
                log.error("Error handling write to client", e);
                closeConnection(key);
            }
        }
    }

    /**
     * Closes a client connection and cancels its selection key.
     *
     * @param key the selection key for the connection to close
     */
    private void closeConnection(final SelectionKey key) {
        try {
            key.channel().close();
        } catch (final IOException e) {
            log.debug("Error closing connection", e);
        }
    }

    /**
     * Stops the server and closes all connections.
     *
     * <p>This method is idempotent - calling it multiple times has no additional effect.</p>
     *
     * <p>The method will wait up to 5 seconds for the server thread to terminate gracefully.</p>
     */
    public void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        log.info("Stopping Kafkaesque server");

        try {
            if (selector != null) {
                selector.close();
            }
            if (serverChannel != null) {
                serverChannel.close();
            }
            if (serverThread != null) {
                serverThread.join(5000);
            }
        } catch (final IOException | InterruptedException e) {
            log.warn("Error during server shutdown", e);
        }

        log.info("Kafkaesque server stopped");
    }

    /**
     * Closes the server by calling {@link #stop()}.
     *
     * <p>This method is provided to implement {@link AutoCloseable},
     * allowing the server to be used in try-with-resources statements.</p>
     */
    @Override
    public void close() {
        stop();
    }

    // Topic management

    /**
     * Pre-registers a topic with the given configuration and compression.
     *
     * <p>FetchResponses for this topic will use the specified compression codec,
     * allowing consumer applications under test to be verified against compressed messages.</p>
     *
     * @param name              the topic name
     * @param numPartitions     the number of partitions
     * @param replicationFactor the replication factor
     * @param compression       the compression to apply when serving FetchResponses
     */
    public void createTopic(
            final String name, final int numPartitions,
            final short replicationFactor, final Compression compression) {
        protocolHandler.createTopic(name, numPartitions, replicationFactor, compression);
    }

    /**
     * Pre-registers a topic with the given configuration.
     *
     * <p>FetchResponses for this topic will use {@link Compression#NONE}.</p>
     *
     * @param name              the topic name
     * @param numPartitions     the number of partitions
     * @param replicationFactor the replication factor
     */
    public void createTopic(final String name, final int numPartitions, final short replicationFactor) {
        createTopic(name, numPartitions, replicationFactor, Compression.NONE);
    }

    // Event retrieval methods

    /**
     * Gets all records published to a specific topic and partition.
     *
     * @param topic the topic name
     * @param partition the partition index
     * @return unmodifiable list of records (empty if none exist)
     */
    public java.util.List<StoredRecord> getRecords(final String topic, final int partition) {
        return protocolHandler.getEventStore().getRecords(topic, partition);
    }

    /**
     * Gets all records published to a specific topic across all partitions.
     *
     * @param topic the topic name
     * @return unmodifiable list of records (empty if none exist)
     */
    public java.util.List<StoredRecord> getRecordsByTopic(final String topic) {
        return protocolHandler.getEventStore().getRecordsByTopic(topic);
    }

    /**
     * Gets all records published to a specific topic with a specific key.
     *
     * @param topic the topic name
     * @param key the key to match (nullable)
     * @return unmodifiable list of records (empty if none exist)
     */
    public java.util.List<StoredRecord> getRecordsByTopicAndKey(final String topic, final String key) {
        return protocolHandler.getEventStore().getRecordsByTopicAndKey(topic, key);
    }

    /**
     * Gets all records published across all topics and partitions.
     *
     * @return unmodifiable list of all records
     */
    public java.util.List<StoredRecord> getAllRecords() {
        return protocolHandler.getEventStore().getAllRecords();
    }

    /**
     * Finds records matching a specific predicate.
     *
     * @param predicate the filter predicate
     * @return unmodifiable list of matching records
     */
    public java.util.List<StoredRecord> findRecords(final java.util.function.Predicate<StoredRecord> predicate) {
        return protocolHandler.getEventStore().findRecords(predicate);
    }

    /**
     * Gets the total number of records published across all topics and partitions.
     *
     * @return the total record count
     */
    public long getTotalRecordCount() {
        return protocolHandler.getEventStore().getTotalRecordCount();
    }

    /**
     * Gets the number of records published to a specific topic.
     *
     * @param topic the topic name
     * @return the record count for the topic
     */
    public long getRecordCount(final String topic) {
        return protocolHandler.getEventStore().getRecordCount(topic);
    }

    /**
     * Gets the number of records published to a specific topic and partition.
     *
     * @param topic the topic name
     * @param partition the partition index
     * @return the record count for the partition
     */
    public long getRecordCount(final String topic, final int partition) {
        return protocolHandler.getEventStore().getRecordCount(topic, partition);
    }

    /**
     * Gets all known topics that have received published records.
     *
     * @return unmodifiable list of topic names
     */
    public java.util.List<String> getKnownTopics() {
        return protocolHandler.getEventStore().getTopics();
    }

    /**
     * Clears all stored records.
     *
     * <p>This is useful for testing scenarios where you want to reset state
     * between test cases while keeping the server running.</p>
     */
    public void clearRecords() {
        protocolHandler.getEventStore().clear();
    }
}
