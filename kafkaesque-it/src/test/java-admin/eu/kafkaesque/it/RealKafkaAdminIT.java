package eu.kafkaesque.it;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the admin-related Kafka behavior test suite against a real Kafka broker
 * provisioned by Testcontainers.
 *
 * <p>This test class covers admin API tests that require {@code kafka-clients} 3.5 or
 * later. It is only compiled when {@code kafka.api.level} is 3 or higher.</p>
 *
 * @see AbstractKafkaAdminBehaviorIT
 * @see KafkaesqueAdminIT
 */
@Testcontainers
class RealKafkaAdminIT extends AbstractKafkaAdminBehaviorIT {

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.8.0")
    ).withEnv("KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS", "1000")
     .withEnv("KAFKA_LOG_CLEANER_BACKOFF_MS", "100")
     .withEnv("KAFKA_LOG_ROLL_MS", "2000")
     .withEnv("KAFKA_LOG_ROLL_JITTER_MS", "0")
     .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer")
     .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "true")
     .withEnv("KAFKA_SUPER_USERS", "User:ANONYMOUS");

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
