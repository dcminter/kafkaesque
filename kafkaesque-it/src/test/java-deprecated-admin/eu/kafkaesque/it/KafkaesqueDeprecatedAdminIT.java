package eu.kafkaesque.it;

import eu.kafkaesque.core.KafkaesqueServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Runs the deprecated admin API test against the Kafkaesque mock server.
 *
 * <p>This test class is only compiled when {@code kafka.api.level} is 3 (i.e. for
 * 3.5.x through 3.x clients), because the deprecated {@code alterConfigs} API was
 * removed in {@code kafka-clients} 4.0.</p>
 *
 * @see AbstractDeprecatedAdminBehaviorIT
 * @see RealKafkaDeprecatedAdminIT
 */
@Slf4j
class KafkaesqueDeprecatedAdminIT extends AbstractDeprecatedAdminBehaviorIT {

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
        log.info("Kafkaesque deprecated-admin-test server started on {}", server.getBootstrapServers());
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
