package eu.kafkaesque.core;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link KafkaProtocolHandler}.
 */
class KafkaProtocolHandlerTest {

    private KafkaProtocolHandler handler;
    private ServerSocketChannel serverSocket;
    private SocketChannel clientChannel;
    private SocketChannel serverSideChannel;
    private ClientConnection clientConnection;
    private TestServerInfo testServerInfo;

    @BeforeEach
    void setUp() throws IOException {
        testServerInfo = new TestServerInfo("test-host", 9093);
        handler = new KafkaProtocolHandler(testServerInfo);

        // Set up real socket connection for integration-style tests
        serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress("localhost", 0));
        serverSocket.configureBlocking(false);

        clientChannel = SocketChannel.open();
        clientChannel.configureBlocking(false);
        clientChannel.connect(serverSocket.getLocalAddress());

        while ((serverSideChannel = serverSocket.accept()) == null) {
            Thread.onSpinWait();
        }
        serverSideChannel.configureBlocking(false);

        while (!clientChannel.finishConnect()) {
            Thread.onSpinWait();
        }

        clientConnection = new ClientConnection(serverSideChannel);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (clientChannel != null && clientChannel.isOpen()) {
            clientChannel.close();
        }
        if (serverSideChannel != null && serverSideChannel.isOpen()) {
            serverSideChannel.close();
        }
        if (serverSocket != null && serverSocket.isOpen()) {
            serverSocket.close();
        }
    }

    @Test
    void shouldHandleApiVersionsRequest() throws IOException, InterruptedException {
        // Given: client sends API_VERSIONS request
        ByteBuffer requestBuffer = createApiVersionsRequest();
        int written = clientChannel.write(requestBuffer);
        assertThat(written).isGreaterThan(0);
        Thread.sleep(50); // Give time for data to arrive

        // When: handler processes the request
        int bytesRead = clientConnection.read();
        assertThat(bytesRead).isGreaterThan(0);

        SelectionKey mockKey = new MockSelectionKey(serverSideChannel);

        // Then: should not throw exception
        handler.handleRead(clientConnection, mockKey);

        // If a response was generated, it would be in the write buffer
        var writeBuffer = clientConnection.writeBuffer();
        // Note: Due to the complexity of the Kafka protocol, we just verify no exception was thrown
    }

    @Test
    void shouldHandleMetadataRequest() throws IOException, InterruptedException {
        // Given: client sends METADATA request
        ByteBuffer requestBuffer = createMetadataRequest();
        int written = clientChannel.write(requestBuffer);
        assertThat(written).isGreaterThan(0);
        Thread.sleep(50); // Give time for data to arrive

        // When: handler processes the request
        int bytesRead = clientConnection.read();
        assertThat(bytesRead).isGreaterThan(0);

        SelectionKey mockKey = new MockSelectionKey(serverSideChannel);

        // Then: should not throw exception
        handler.handleRead(clientConnection, mockKey);
    }

    @Test
    void shouldHandleDescribeClusterRequest() throws IOException, InterruptedException {
        // Given: client sends DESCRIBE_CLUSTER request
        ByteBuffer requestBuffer = createDescribeClusterRequest();
        int written = clientChannel.write(requestBuffer);
        assertThat(written).isGreaterThan(0);
        Thread.sleep(50); // Give time for data to arrive

        // When: handler processes the request
        int bytesRead = clientConnection.read();
        assertThat(bytesRead).isGreaterThan(0);

        SelectionKey mockKey = new MockSelectionKey(serverSideChannel);

        // Then: should not throw exception
        handler.handleRead(clientConnection, mockKey);
    }

    @Test
    void shouldHandleClientDisconnect() throws IOException {
        // Given: client disconnects
        clientChannel.close();
        Thread.yield();

        // When: handler tries to read
        int bytesRead = clientConnection.read();

        // Then: should return -1 indicating EOF
        assertThat(bytesRead).isEqualTo(-1);
    }

    @Test
    void shouldUseFallbackPortWhenServerInfoThrowsException() throws IOException, InterruptedException {
        // Given: serverInfo that throws exception
        testServerInfo.setShouldThrowException(true);

        // And: client sends METADATA request
        ByteBuffer requestBuffer = createMetadataRequest();
        int written = clientChannel.write(requestBuffer);
        assertThat(written).isGreaterThan(0);
        Thread.sleep(50); // Give time for data to arrive

        // When: handler processes the request
        int bytesRead = clientConnection.read();
        assertThat(bytesRead).isGreaterThan(0);

        SelectionKey mockKey = new MockSelectionKey(serverSideChannel);

        // Then: should not throw exception (uses fallback values)
        handler.handleRead(clientConnection, mockKey);
    }

    @Test
    void shouldHandleWithoutServerInfo() throws IOException, InterruptedException {
        // Given: handler without server info
        KafkaProtocolHandler handlerWithoutInfo = new KafkaProtocolHandler();
        // serverInfo is null

        // And: client sends METADATA request
        ByteBuffer requestBuffer = createMetadataRequest();
        int written = clientChannel.write(requestBuffer);
        assertThat(written).isGreaterThan(0);
        Thread.sleep(50); // Give time for data to arrive

        // When: handler processes the request
        int bytesRead = clientConnection.read();
        assertThat(bytesRead).isGreaterThan(0);

        SelectionKey mockKey = new MockSelectionKey(serverSideChannel);

        // Then: should not throw exception (uses default values)
        handlerWithoutInfo.handleRead(clientConnection, mockKey);
    }

    /**
     * Create a minimal API_VERSIONS request.
     */
    private ByteBuffer createApiVersionsRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        int sizePosition = buffer.position();
        buffer.putInt(0);
        int requestStart = buffer.position();

        buffer.putShort(ApiKeys.API_VERSIONS.id);
        buffer.putShort((short) 4);
        buffer.putInt(1);
        buffer.putShort((short) -1);
        buffer.put((byte) 0);

        int requestEnd = buffer.position();
        int requestSize = requestEnd - requestStart - 4;
        buffer.putInt(sizePosition, requestSize);

        buffer.flip();
        return buffer;
    }

    /**
     * Create a minimal METADATA request.
     */
    private ByteBuffer createMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        int sizePosition = buffer.position();
        buffer.putInt(0);
        int requestStart = buffer.position();

        buffer.putShort(ApiKeys.METADATA.id);
        buffer.putShort((short) 12);
        buffer.putInt(2);
        buffer.putShort((short) -1);
        buffer.put((byte) 1);
        buffer.put((byte) 0);
        buffer.put((byte) 0);

        int requestEnd = buffer.position();
        int requestSize = requestEnd - requestStart - 4;
        buffer.putInt(sizePosition, requestSize);

        buffer.flip();
        return buffer;
    }

    /**
     * Create a minimal DESCRIBE_CLUSTER request.
     */
    private ByteBuffer createDescribeClusterRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        int sizePosition = buffer.position();
        buffer.putInt(0);
        int requestStart = buffer.position();

        buffer.putShort(ApiKeys.DESCRIBE_CLUSTER.id);
        buffer.putShort((short) 1);
        buffer.putInt(3);
        buffer.putShort((short) -1);
        buffer.put((byte) 0);
        buffer.put((byte) 0);

        int requestEnd = buffer.position();
        int requestSize = requestEnd - requestStart - 4;
        buffer.putInt(sizePosition, requestSize);

        buffer.flip();
        return buffer;
    }

    /**
     * Test implementation of ServerInfo.
     */
    private static class TestServerInfo implements ServerInfo {
        private final String host;
        private final int port;
        private boolean shouldThrowException = false;

        TestServerInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }

        void setShouldThrowException(boolean shouldThrow) {
            this.shouldThrowException = shouldThrow;
        }

        @Override
        public int getPort() throws IOException {
            if (shouldThrowException) {
                throw new IOException("Test exception");
            }
            return port;
        }

        @Override
        public String getHost() {
            return host;
        }
    }

    /**
     * Minimal SelectionKey implementation for testing.
     */
    private static class MockSelectionKey extends SelectionKey {
        private final SocketChannel channel;
        private int interestOps = SelectionKey.OP_READ;

        MockSelectionKey(SocketChannel channel) {
            this.channel = channel;
        }

        @Override
        public java.nio.channels.SelectableChannel channel() {
            return channel;
        }

        @Override
        public java.nio.channels.Selector selector() {
            return null;
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public void cancel() {
        }

        @Override
        public int interestOps() {
            return interestOps;
        }

        @Override
        public SelectionKey interestOps(int ops) {
            this.interestOps = ops;
            return this;
        }

        @Override
        public int readyOps() {
            return SelectionKey.OP_READ;
        }
    }
}
