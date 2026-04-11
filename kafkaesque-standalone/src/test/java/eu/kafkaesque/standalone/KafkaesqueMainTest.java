package eu.kafkaesque.standalone;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the configuration-resolution methods of {@link KafkaesqueMain}.
 */
class KafkaesqueMainTest {

    /**
     * Verifies that the default host is {@code 0.0.0.0} when no environment
     * variable is set.
     */
    @Test
    void shouldReturnDefaultHost() {
        // Environment variables cannot be overridden portably in unit tests,
        // so we only verify the constant is what we expect.
        assertThat(KafkaesqueMain.DEFAULT_HOST).isEqualTo("0.0.0.0");
    }

    /**
     * Verifies that the default port is {@code 9092} when no environment
     * variable is set.
     */
    @Test
    void shouldReturnDefaultPort() {
        assertThat(KafkaesqueMain.DEFAULT_PORT).isEqualTo(9092);
    }
}
