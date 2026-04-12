package eu.kafkaesque.it;

import eu.kafkaesque.core.KafkaesqueServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Runs the admin-related Kafka behavior test suite against the Kafkaesque mock server.
 *
 * <p>This test class covers admin API tests that require {@code kafka-clients} 3.5 or
 * later. It is only compiled when {@code kafka.api.level} is 3 or higher.</p>
 *
 * @see AbstractKafkaAdminBehaviorIT
 * @see RealKafkaAdminIT
 */
@Slf4j
class KafkaesqueAdminIT extends AbstractKafkaAdminBehaviorIT {

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
        log.info("Kafkaesque admin-test server started on {}", server.getBootstrapServers());
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
