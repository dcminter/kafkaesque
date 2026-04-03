package eu.kafkaesque.it;

import eu.kafkaesque.core.KafkaesqueServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Runs the shared Kafka behavior test suite against the Kafkaesque mock server.
 *
 * <p>The server is started once for the entire test class (in {@link #startServer()})
 * rather than per-test, because the abstract base class's {@code @BeforeEach} calls
 * {@link #getBootstrapServers()} before any subclass {@code @BeforeEach} would run.
 * Using {@code @BeforeAll} ensures the server is ready in time.</p>
 *
 * <p>Tests that are not yet supported by the mock will fail here but pass in
 * {@link RealKafkaIT}, making it easy to track implementation progress.</p>
 *
 * @see AbstractKafkaBehaviorIT
 * @see RealKafkaIT
 */
@Slf4j
class KafkaesqueIT extends AbstractKafkaBehaviorIT {

    private static KafkaesqueServer server;

    /**
     * Starts the Kafkaesque mock server on an ephemeral port before any tests run.
     *
     * @throws Exception if the server cannot be started
     */
    @BeforeAll
    static void startServer() throws Exception {
        server = new KafkaesqueServer("localhost", 0);
        server.start();
        log.info("Kafkaesque server started on {}", server.getBootstrapServers());
    }

    /**
     * Stops the Kafkaesque mock server after all tests have completed.
     */
    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @return the bootstrap servers address of the Kafkaesque mock server
     * @throws Exception if the port cannot be determined
     */
    @Override
    protected String getBootstrapServers() throws Exception {
        return server.getBootstrapServers();
    }
}
