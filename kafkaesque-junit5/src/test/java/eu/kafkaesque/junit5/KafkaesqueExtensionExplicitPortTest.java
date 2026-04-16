package eu.kafkaesque.junit5;

import eu.kafkaesque.core.KafkaesqueServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.io.IOException;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates that the {@link Kafkaesque} annotation's {@code port} attribute correctly binds
 * the {@link KafkaesqueServer} to the specified port.
 */
@Kafkaesque(port = 19092)
@DisabledIf("isPortInUse")
class KafkaesqueExtensionExplicitPortTest {

    /**
     * Checks whether the chosen test port is already in use. If so, the test is skipped
     * to avoid failures when another process has bound the port.
     *
     * @return {@code true} if port 19092 is already in use
     */
    static boolean isPortInUse() {
        try (var ignored = new ServerSocket(19092)) {
            return false;
        } catch (final IOException e) {
            return true;
        }
    }

    /**
     * Verifies that the server is listening on the explicitly configured port.
     *
     * @param server the injected {@link KafkaesqueServer}
     * @throws IOException if the server address cannot be determined
     */
    @Test
    void serverShouldListenOnExplicitPort(final KafkaesqueServer server) throws IOException {
        assertThat(server.getPort()).isEqualTo(19092);
        assertThat(server.getBootstrapServers()).isEqualTo("localhost:19092");
    }
}
