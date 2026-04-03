package eu.kafkaesque.it;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the shared Kafka behavior test suite against a real Kafka broker
 * provisioned by Testcontainers.
 *
 * <p>This serves as the reference implementation: if the tests pass here, the
 * expected behavior is verified as correct against a real Kafka service,
 * giving confidence that the {@link KafkaesqueIT} assertions are meaningful.</p>
 *
 * @see AbstractKafkaBehaviorIT
 * @see KafkaesqueIT
 */
@Testcontainers
class RealKafkaIT extends AbstractKafkaBehaviorIT {

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.8.0")
    );

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
