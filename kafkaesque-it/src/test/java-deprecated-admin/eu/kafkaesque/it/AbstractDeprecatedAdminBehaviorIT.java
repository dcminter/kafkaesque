package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static java.util.List.of;

/**
 * Abstract base class defining a test for the deprecated
 * {@link AdminClient#alterConfigs(Map)} API.
 *
 * <p>This API was removed in {@code kafka-clients} 4.0, so this test is only compiled
 * when {@code kafka.api.level} is 3 (i.e. for 3.5.x through 3.x clients). For 4.x+
 * clients, the replacement {@code incrementalAlterConfigs} API is tested in
 * {@code AbstractAdminBehaviorIT} instead.</p>
 *
 * <p>Subclasses must implement {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractDeprecatedAdminBehaviorIT {

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Verifies that topic configuration can be altered via the deprecated
     * {@link AdminClient#alterConfigs} API without error.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    @SuppressWarnings("deprecation")
    void shouldAlterTopicConfigViaAdminClient() throws Exception {
        // Given
        final String topicName = "alter-config-topic-" + UUID.randomUUID();
        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            // When
            final var resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            final var config = new org.apache.kafka.clients.admin.Config(of(
                new ConfigEntry("retention.ms", "60000")));
            adminClient.alterConfigs(Map.of(resource, config)).all().get();

            // Then — no exception means success
            log.info("Altered config for topic {}", topicName);
        }
    }
}
