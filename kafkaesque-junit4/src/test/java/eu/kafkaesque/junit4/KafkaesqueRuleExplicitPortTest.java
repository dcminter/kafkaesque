package eu.kafkaesque.junit4;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.io.IOException;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates that {@link KafkaesqueRule} correctly binds the server to an explicitly configured
 * port when one is specified via the builder.
 */
@DisabledIf("isPortInUse")
class KafkaesqueRuleExplicitPortTest {

    /** A fixed port number used for testing explicit port binding. */
    private static final int TEST_PORT = 19093;

    private final KafkaesqueRule kafka = KafkaesqueRule.builder()
        .port(TEST_PORT)
        .build();

    /**
     * Checks whether the chosen test port is already in use. If so, the test is skipped
     * to avoid failures when another process has bound the port.
     *
     * @return {@code true} if the test port is already in use
     */
    static boolean isPortInUse() {
        try (var ignored = new ServerSocket(TEST_PORT)) {
            return false;
        } catch (final IOException e) {
            return true;
        }
    }

    @BeforeEach
    void startServer() throws Throwable {
        kafka.before();
    }

    @AfterEach
    void stopServer() {
        kafka.after();
    }

    /**
     * Verifies that the server is listening on the explicitly configured port.
     *
     * @throws IOException if the server address cannot be determined
     */
    @Test
    void serverShouldListenOnExplicitPort() throws IOException {
        assertThat(kafka.getServer().getPort()).isEqualTo(TEST_PORT);
        assertThat(kafka.getBootstrapServers()).isEqualTo("localhost:" + TEST_PORT);
    }
}
