package eu.kafkaesque.core.connection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ClientConnection}.
 */
class ClientConnectionTest {

    private ServerSocketChannel serverSocket;
    private SocketChannel clientChannel;
    private SocketChannel serverSideChannel;
    private ClientConnection clientConnection;

    @BeforeEach
    void setUp() throws IOException {
        // Create a server socket on ephemeral port
        serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress("localhost", 0));
        serverSocket.configureBlocking(false);

        // Connect from client side
        clientChannel = SocketChannel.open();
        clientChannel.configureBlocking(false);
        clientChannel.connect(serverSocket.getLocalAddress());

        // Accept on server side
        while ((serverSideChannel = serverSocket.accept()) == null) {
            Thread.onSpinWait();
        }
        serverSideChannel.configureBlocking(false);

        // Finish client connection
        while (!clientChannel.finishConnect()) {
            Thread.onSpinWait();
        }

        // Create ClientConnection wrapping the server-side channel
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
    void shouldCreateConnectionWithBuffers() {
        assertThat(clientConnection.channel()).isNotNull();
        assertThat(clientConnection.readBuffer()).isNotNull();
        assertThat(clientConnection.writeBuffer()).isNotNull();
    }

    @Test
    void shouldReadDataFromChannel() throws IOException {
        // Given: client sends some data
        String testData = "Hello, Kafkaesque!";
        ByteBuffer sendBuffer = ByteBuffer.wrap(testData.getBytes());
        while (sendBuffer.hasRemaining()) {
            clientChannel.write(sendBuffer);
        }

        // When: reading from connection
        // Give OS time to transfer data
        Thread.yield();
        int bytesRead = clientConnection.read();

        // Then: data should be in read buffer
        assertThat(bytesRead).isGreaterThan(0);
        var readBuffer = clientConnection.readBuffer();
        readBuffer.flip();
        var receivedBytes = new byte[readBuffer.remaining()];
        readBuffer.get(receivedBytes);
        assertThat(new String(receivedBytes)).isEqualTo(testData);
    }

    @Test
    void shouldWriteDataToChannel() throws IOException {
        // Given: data in write buffer
        var testData = "Response from Kafkaesque";
        var writeBuffer = clientConnection.writeBuffer();
        writeBuffer.put(testData.getBytes());
        writeBuffer.flip();

        // When: writing to channel
        var bytesWritten = clientConnection.write();

        // Then: data should be written
        assertThat(bytesWritten).isGreaterThan(0);

        // And: client should receive the data
        var receiveBuffer = ByteBuffer.allocate(1024);
        Thread.yield(); // Give OS time to transfer data
        var bytesReceived = clientChannel.read(receiveBuffer);
        assertThat(bytesReceived).isGreaterThan(0);

        receiveBuffer.flip();
        var receivedBytes = new byte[receiveBuffer.remaining()];
        receiveBuffer.get(receivedBytes);
        assertThat(new String(receivedBytes)).isEqualTo(testData);
    }

    @Test
    void shouldHandleClosedConnection() throws IOException {
        // Given: client closes connection
        clientChannel.close();

        // When: reading from closed connection
        // Give OS time to propagate close
        Thread.yield();
        int bytesRead = clientConnection.read();

        // Then: should return -1 indicating EOF
        assertThat(bytesRead).isEqualTo(-1);
    }

    @Test
    void shouldHaveInitiallyEmptyBuffers() {
        var readBuffer = clientConnection.readBuffer();
        var writeBuffer = clientConnection.writeBuffer();

        assertThat(readBuffer.position()).isEqualTo(0);
        assertThat(writeBuffer.position()).isEqualTo(0);
        assertThat(readBuffer.capacity()).isEqualTo(8192);
        assertThat(writeBuffer.capacity()).isEqualTo(8192);
    }
}
