package eu.kafkaesque.it;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the deprecated admin API test against a real Kafka broker provisioned by
 * Testcontainers.
 *
 * <p>This test class is only compiled when {@code kafka.api.level} is 3 (i.e. for
 * 3.5.x through 3.x clients), because the deprecated {@code alterConfigs} API was
 * removed in {@code kafka-clients} 4.0.</p>
 *
 * @see AbstractDeprecatedAdminBehaviorIT
 * @see KafkaesqueDeprecatedAdminIT
 */
@Testcontainers
class RealKafkaDeprecatedAdminIT extends AbstractDeprecatedAdminBehaviorIT {

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.8.0")
    ).withEnv("KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS", "1000")
     .withEnv("KAFKA_LOG_CLEANER_BACKOFF_MS", "100")
     .withEnv("KAFKA_LOG_ROLL_MS", "2000")
     .withEnv("KAFKA_LOG_ROLL_JITTER_MS", "0");

    /**
     * {@inheritDoc}
     *
     * @return the bootstrap servers address provided by the Testcontainers Kafka broker
     */
    @Override
    protected String getBootstrapServers() {
        return kafka.getBootstrapServers();
    }
}
