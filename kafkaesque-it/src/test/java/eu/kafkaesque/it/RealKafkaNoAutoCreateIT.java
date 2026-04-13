package eu.kafkaesque.it;

import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the no-auto-create-topics test suite against a real Kafka broker configured with
 * {@code auto.create.topics.enable=false}.
 *
 * <p>This serves as the reference implementation: if the tests pass here, the expected
 * behaviour is verified as correct against a real Kafka service, giving confidence that
 * the {@link KafkaesqueNoAutoCreateIT} assertions are meaningful.</p>
 *
 * <p>Disabled when {@code kafka.api.level=1} because the Testcontainers broker
 * (cp-kafka 7.8) is incompatible with very old client libraries (1.x&ndash;2.x).</p>
 *
 * @see AbstractNoAutoCreateTopicsBehaviorIT
 * @see KafkaesqueNoAutoCreateIT
 */
@Testcontainers
@DisabledIfSystemProperty(named = "kafka.api.level", matches = "1")
class RealKafkaNoAutoCreateIT extends AbstractNoAutoCreateTopicsBehaviorIT {

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.8.0")
    ).withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

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
