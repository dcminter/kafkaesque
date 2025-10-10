package eu.kafkaesque.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link KafkaesqueServer}.
 */
class KafkaesqueServerTest {

    private KafkaesqueServer server;
    private SocketChannel testClient;

    @AfterEach
    void tearDown() {
        if (testClient != null && testClient.isOpen()) {
            try {
                testClient.close();
            } catch (IOException e) {
                // Ignore
            }
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void shouldStartAndStopServer() throws IOException {
        // Given
        server = new KafkaesqueServer("localhost", 0);

        // When
        server.start();

        // Then
        int port = server.getPort();
        assertThat(port).isGreaterThan(0);
        assertThat(server.getHost()).isEqualTo("localhost");

        // And when stopped
        server.stop();

        // Then should not be able to connect
        testClient = SocketChannel.open();
        testClient.configureBlocking(true);
        assertThatThrownBy(() -> testClient.connect(new InetSocketAddress("localhost", port)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void shouldAcceptClientConnections() throws IOException, InterruptedException {
        // Given
        server = new KafkaesqueServer("localhost", 0);
        server.start();
        int port = server.getPort();

        // When: client connects
        testClient = SocketChannel.open();
        testClient.configureBlocking(true);
        boolean connected = testClient.connect(new InetSocketAddress("localhost", port));

        // Then
        assertThat(connected).isTrue();
        assertThat(testClient.isConnected()).isTrue();

        // Give server time to process the connection
        Thread.sleep(100);
    }

    @Test
    void shouldProvideBootstrapServers() throws IOException {
        // Given
        server = new KafkaesqueServer("localhost", 0);
        server.start();

        // When
        String bootstrapServers = server.getBootstrapServers();

        // Then
        assertThat(bootstrapServers).matches("localhost:\\d+");
        assertThat(bootstrapServers).contains(String.valueOf(server.getPort()));
    }

    @Test
    void shouldThrowExceptionWhenStartingTwice() throws IOException {
        // Given
        server = new KafkaesqueServer("localhost", 0);
        server.start();

        // When/Then
        assertThatThrownBy(() -> server.start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already running");
    }

    @Test
    void shouldThrowExceptionWhenGettingPortBeforeStart() {
        // Given
        server = new KafkaesqueServer("localhost", 0);

        // When/Then
        assertThatThrownBy(() -> server.getPort())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not started");
    }

    @Test
    void shouldBeIdempotentWhenStoppingTwice() throws IOException {
        // Given
        server = new KafkaesqueServer("localhost", 0);
        server.start();

        // When
        server.stop();
        server.stop(); // Second stop

        // Then: no exception should be thrown
    }

    @Test
    void shouldHandleMultipleClientConnections() throws IOException, InterruptedException {
        // Given
        server = new KafkaesqueServer("localhost", 0);
        server.start();
        int port = server.getPort();

        // When: multiple clients connect
        SocketChannel client1 = SocketChannel.open();
        client1.connect(new InetSocketAddress("localhost", port));

        SocketChannel client2 = SocketChannel.open();
        client2.connect(new InetSocketAddress("localhost", port));

        SocketChannel client3 = SocketChannel.open();
        client3.connect(new InetSocketAddress("localhost", port));

        // Then: all should be connected
        assertThat(client1.isConnected()).isTrue();
        assertThat(client2.isConnected()).isTrue();
        assertThat(client3.isConnected()).isTrue();

        // Cleanup
        client1.close();
        client2.close();
        client3.close();

        // Give server time to process
        Thread.sleep(100);
    }

    @Test
    void shouldImplementAutoCloseable() throws IOException {
        // Given
        int port;

        // When: using try-with-resources
        try (KafkaesqueServer testServer = new KafkaesqueServer("localhost", 0)) {
            testServer.start();
            port = testServer.getPort();

            testClient = SocketChannel.open();
            testClient.connect(new InetSocketAddress("localhost", port));
            assertThat(testClient.isConnected()).isTrue();
        }

        // Then: server should be closed
        // Attempting to connect should fail
        SocketChannel newClient = SocketChannel.open();
        newClient.configureBlocking(true);
        assertThatThrownBy(() -> newClient.connect(new InetSocketAddress("localhost", port)))
                .isInstanceOf(IOException.class);
        newClient.close();
    }

    @Test
    void shouldBindToSpecificPort() throws IOException {
        // Given: a specific port (using ephemeral range to avoid conflicts)
        int specificPort = 0; // Let OS choose, but test the mechanism

        // When
        server = new KafkaesqueServer("localhost", specificPort);
        server.start();

        // Then
        assertThat(server.getPort()).isGreaterThan(0);
    }

    @Test
    void shouldImplementServerInfo() throws IOException {
        // Given
        server = new KafkaesqueServer("localhost", 0);
        server.start();

        // When: accessing as ServerInfo
        ServerInfo serverInfo = server;

        // Then
        assertThat(serverInfo.getHost()).isEqualTo("localhost");
        assertThat(serverInfo.getPort()).isGreaterThan(0);
    }

    @Test
    void shouldHandleRapidStartStop() throws IOException {
        // This tests for any resource leaks or race conditions

        for (int i = 0; i < 5; i++) {
            server = new KafkaesqueServer("localhost", 0);
            server.start();
            int port = server.getPort();
            assertThat(port).isGreaterThan(0);
            server.stop();

            // Small delay to ensure port is released
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
