package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract base class defining the admin-client integration test suite.
 *
 * <p>Covers cluster connectivity, topic creation, and topic description via
 * {@link AdminClient}. Subclasses must implement {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractAdminBehaviorIT {

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Verifies that an {@link AdminClient} can connect to the broker and retrieve
     * cluster metadata.
     *
     * @throws Exception if the admin client cannot connect
     */
    @Test
    void shouldEstablishConnectionToKafkaBroker() throws Exception {
        // Given
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        // When / Then
        try (final AdminClient adminClient = AdminClient.create(props)) {
            final DescribeClusterResult clusterResult = adminClient.describeCluster();
            final String clusterId = clusterResult.clusterId().get();
            final int nodeCount = clusterResult.nodes().get().size();

            assertThat(clusterId).isNotNull();
            assertThat(nodeCount).isGreaterThan(0);

            log.info("Connected to cluster: id={}, nodes={}", clusterId, nodeCount);
        }
    }

    /**
     * Verifies that a new topic can be created via the {@link AdminClient} and that
     * the resulting topic is visible when listing topics and describable with the
     * expected partition count and replication factor.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldCreateTopicViaAdminClient() throws Exception {
        // Given
        final String newTopicName = "admin-created-topic-" + UUID.randomUUID();
        final int partitionCount = 2;
        final short replicationFactor = 1;
        final NewTopic newTopic = new NewTopic(newTopicName, partitionCount, replicationFactor);

        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            // When
            adminClient.createTopics(of(newTopic)).all().get();

            // Then – the topic appears in the listing
            final Set<String> topicNames = adminClient.listTopics().names().get();
            assertThat(topicNames).contains(newTopicName);

            // And – the topic has the expected configuration
            final Map<String, TopicDescription> descriptions =
                adminClient.describeTopics(of(newTopicName)).allTopicNames().get();
            final TopicDescription description = descriptions.get(newTopicName);
            assertThat(description).isNotNull();
            assertThat(description.name()).isEqualTo(newTopicName);
            assertThat(description.partitions()).hasSize(partitionCount);

            log.info("Created topic: name={}, partitions={}", description.name(), description.partitions().size());
        }
    }

    /**
     * Verifies that multiple topics created via the admin API can all be described in a
     * single {@code describeTopics} call, and that each is returned with the correct
     * partition count.
     *
     * <p>This test directly exercises the DESCRIBE_TOPIC_PARTITIONS API path used by
     * {@code AdminClient.describeTopics()} in Kafka 3.7 and above.</p>
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldDescribeMultipleTopicsViaDescribeTopicPartitions() throws Exception {
        // Given
        final String topicA = "describe-tp-topic-a-" + UUID.randomUUID();
        final String topicB = "describe-tp-topic-b-" + UUID.randomUUID();

        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(of(
                new NewTopic(topicA, 1, (short) 1),
                new NewTopic(topicB, 4, (short) 1)
            )).all().get();

            // When
            final Map<String, TopicDescription> descriptions =
                adminClient.describeTopics(of(topicA, topicB)).allTopicNames().get();

            // Then
            assertThat(descriptions).containsKeys(topicA, topicB);
            assertThat(descriptions.get(topicA).partitions()).hasSize(1);
            assertThat(descriptions.get(topicB).partitions()).hasSize(4);

            log.info("Described topics: {}={}p, {}={}p",
                topicA, descriptions.get(topicA).partitions().size(),
                topicB, descriptions.get(topicB).partitions().size());
        }
    }
}
