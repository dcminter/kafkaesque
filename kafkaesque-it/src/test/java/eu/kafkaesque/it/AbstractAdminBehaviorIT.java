package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

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

    /**
     * Verifies that a topic can be deleted via the {@link AdminClient} and that
     * it no longer appears in the topic listing afterwards.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldDeleteTopicViaAdminClient() throws Exception {
        // Given
        final String topicName = "delete-topic-" + UUID.randomUUID();
        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            final Set<String> beforeDelete = adminClient.listTopics().names().get();
            assertThat(beforeDelete).contains(topicName);

            // When
            adminClient.deleteTopics(of(topicName)).all().get();

            // Then
            final Set<String> afterDelete = adminClient.listTopics().names().get();
            assertThat(afterDelete).doesNotContain(topicName);

            log.info("Deleted topic: {}", topicName);
        }
    }

    /**
     * Verifies that topic configuration can be described via the {@link AdminClient}
     * and contains the expected standard config entries.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldDescribeTopicConfigViaAdminClient() throws Exception {
        // Given
        final String topicName = "describe-config-topic-" + UUID.randomUUID();
        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            // When
            final var resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            final var configs = adminClient.describeConfigs(of(resource)).all().get();

            // Then
            final var topicConfig = configs.get(resource);
            assertThat(topicConfig).isNotNull();

            final Map<String, ConfigEntry> entries = topicConfig.entries().stream()
                .collect(java.util.stream.Collectors.toMap(ConfigEntry::name, e -> e));
            assertThat(entries).containsKey("cleanup.policy");
            assertThat(entries).containsKey("retention.ms");

            log.info("Described config for topic {}: cleanup.policy={}, retention.ms={}",
                topicName,
                entries.get("cleanup.policy").value(),
                entries.get("retention.ms").value());
        }
    }

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

    /**
     * Verifies that additional partitions can be created for an existing topic via
     * {@link AdminClient#createPartitions}.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldCreateAdditionalPartitionsViaAdminClient() throws Exception {
        // Given
        final String topicName = "create-partitions-topic-" + UUID.randomUUID();
        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(of(new NewTopic(topicName, 2, (short) 1))).all().get();

            // When
            adminClient.createPartitions(Map.of(topicName, NewPartitions.increaseTo(4))).all().get();

            // Then
            final var descriptions = adminClient.describeTopics(of(topicName)).allTopicNames().get();
            assertThat(descriptions.get(topicName).partitions()).hasSize(4);

            log.info("Increased partitions for topic {} to 4", topicName);
        }
    }

    /**
     * Verifies that records before a given offset can be deleted via
     * {@link AdminClient#deleteRecords}.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldDeleteRecordsViaAdminClient() throws Exception {
        // Given
        final String topicName = "delete-records-topic-" + UUID.randomUUID();
        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            // Produce some records
            try (final var producer = KafkaTestClientFactory.createProducer(getBootstrapServers())) {
                for (int i = 0; i < 5; i++) {
                    producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                        topicName, "key-" + i, "value-" + i)).get();
                }
            }

            // When — delete records before offset 3
            final var tp = new org.apache.kafka.common.TopicPartition(topicName, 0);
            final var recordsToDelete = Map.of(tp,
                org.apache.kafka.clients.admin.RecordsToDelete.beforeOffset(3));
            adminClient.deleteRecords(recordsToDelete).all().get();

            // Then — consumer starting from earliest should only see records at offset >= 3
            try (final var consumer = KafkaTestClientFactory.createConsumer(getBootstrapServers())) {
                consumer.assign(of(tp));
                consumer.seekToBeginning(of(tp));
                final var records = consumer.poll(java.time.Duration.ofSeconds(5));
                assertThat(records.count()).isEqualTo(2);
                assertThat(records.iterator().next().offset()).isGreaterThanOrEqualTo(3);
            }

            log.info("Deleted records before offset 3 for topic {}", topicName);
        }
    }

    /**
     * Verifies that leader election can be triggered via the admin API.
     *
     * <p>With a single replica, the preferred leader is already the leader, so the
     * broker returns {@code ELECTION_NOT_NEEDED} for the partition. Both real Kafka
     * and Kafkaesque behave this way.</p>
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldElectLeadersViaAdminClient() throws Exception {
        // Given
        final String topicName = "elect-leaders-topic-" + UUID.randomUUID();
        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            // When — elect preferred leaders for a single-replica topic
            final var tp = new TopicPartition(topicName, 0);
            try {
                adminClient.electLeaders(ElectionType.PREFERRED, Set.of(tp)).all().get();
                // Some Kafka versions may not throw; tolerate success as well
                log.info("Elected leaders for topic {}", topicName);
            } catch (final java.util.concurrent.ExecutionException e) {
                // ElectionNotNeededException is the expected result for single-replica topics
                assertThat(e.getCause())
                    .isInstanceOf(org.apache.kafka.common.errors.ElectionNotNeededException.class);
                log.info("Election not needed for topic {} (expected with single replica)", topicName);
            }
        }
    }

    /**
     * Verifies that log directories can be described via the admin API.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldDescribeLogDirsViaAdminClient() throws Exception {
        // Given
        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            // When
            final var nodes = adminClient.describeCluster().nodes().get();
            final var brokerIds = nodes.stream()
                .map(org.apache.kafka.common.Node::id)
                .collect(java.util.stream.Collectors.toList());
            final var logDirs = adminClient.describeLogDirs(brokerIds).allDescriptions().get();

            // Then
            assertThat(logDirs).isNotEmpty();
            log.info("Described log dirs for {} broker(s)", logDirs.size());
        }
    }

    /**
     * Verifies that offsets can be listed via the admin API.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldListOffsetsViaAdminClient() throws Exception {
        // Given
        final String topicName = "list-offsets-admin-topic-" + UUID.randomUUID();
        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            try (final var producer = KafkaTestClientFactory.createProducer(getBootstrapServers())) {
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                    topicName, "key", "value")).get();
            }

            // When
            final var tp = new TopicPartition(topicName, 0);
            final var offsets = adminClient.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get();

            // Then
            assertThat(offsets).containsKey(tp);
            assertThat(offsets.get(tp).offset()).isGreaterThanOrEqualTo(1);
            log.info("Listed offsets for topic {}: latest={}", topicName, offsets.get(tp).offset());
        }
    }

    /**
     * Verifies that broker features can be described via the admin API.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldDescribeFeaturesViaAdminClient() throws Exception {
        // Given
        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            // When
            final var features = adminClient.describeFeatures().featureMetadata().get();

            // Then
            assertThat(features).isNotNull();
            log.info("Described features: {}", features.supportedFeatures());
        }
    }

    /**
     * Verifies that topic configuration can be incrementally altered via the
     * {@link AdminClient#incrementalAlterConfigs} API without error.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldIncrementalAlterTopicConfigViaAdminClient() throws Exception {
        // Given
        final String topicName = "inc-alter-config-topic-" + UUID.randomUUID();
        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            // When
            final var resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            final var op = new AlterConfigOp(
                new ConfigEntry("retention.ms", "60000"), AlterConfigOp.OpType.SET);
            adminClient.incrementalAlterConfigs(Map.of(resource, of(op))).all().get();

            // Then — no exception means success
            log.info("Incrementally altered config for topic {}", topicName);
        }
    }

    /**
     * Verifies that a topic created with custom configuration values can be described
     * and that the values are returned correctly in the configuration response.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldCreateTopicWithCustomConfigAndVerifyViaDescribeConfigs() throws Exception {
        // Given
        final String topicName = "custom-config-topic-" + UUID.randomUUID();
        final var newTopic = new NewTopic(topicName, 1, (short) 1)
            .configs(Map.of(
                "cleanup.policy", "compact",
                "retention.ms", "86400000"));

        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            // When
            adminClient.createTopics(of(newTopic)).all().get();

            final var resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            final var configs = adminClient.describeConfigs(of(resource)).all().get();

            // Then
            final var topicConfig = configs.get(resource);
            assertThat(topicConfig).isNotNull();

            final Map<String, ConfigEntry> entries = topicConfig.entries().stream()
                .collect(java.util.stream.Collectors.toMap(ConfigEntry::name, e -> e));
            assertThat(entries.get("cleanup.policy").value()).isEqualTo("compact");
            assertThat(entries.get("retention.ms").value()).isEqualTo("86400000");

            log.info("Created topic {} with custom config: cleanup.policy=compact, retention.ms=86400000", topicName);
        }
    }

    /**
     * Verifies that deleting records before a given offset returns the expected low
     * watermark and that a consumer reading from the beginning only sees the
     * remaining records.
     *
     * <p>This complements {@link #shouldDeleteRecordsViaAdminClient()} by also
     * asserting the low watermark returned from the {@code deleteRecords} call itself.</p>
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldReturnCorrectLowWatermarkAfterDeleteRecords() throws Exception {
        // Given
        final String topicName = "delete-records-watermark-topic-" + UUID.randomUUID();
        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            try (final var producer = KafkaTestClientFactory.createProducer(getBootstrapServers())) {
                for (int i = 0; i < 5; i++) {
                    producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                        topicName, "key-" + i, "value-" + i)).get();
                }
            }

            // When — delete records before offset 3
            final var tp = new TopicPartition(topicName, 0);
            final var recordsToDelete = Map.of(tp,
                org.apache.kafka.clients.admin.RecordsToDelete.beforeOffset(3));
            final var result = adminClient.deleteRecords(recordsToDelete)
                .lowWatermarks().get(tp).get();

            // Then — low watermark should be 3
            assertThat(result.lowWatermark()).isEqualTo(3L);

            // And — consumer should only see 2 remaining records
            try (final var consumer = KafkaTestClientFactory.createConsumer(getBootstrapServers())) {
                consumer.assign(of(tp));
                consumer.seekToBeginning(of(tp));
                final var records = consumer.poll(java.time.Duration.ofSeconds(5));
                assertThat(records.count()).isEqualTo(2);
            }

            log.info("Low watermark after deleting records before 3 for topic {}: {}",
                topicName, result.lowWatermark());
        }
    }

    /**
     * Verifies that replica log directory reassignment can be requested via the
     * admin API without error.
     *
     * <p>On a single-broker setup this is effectively a no-op on both real Kafka and
     * Kafkaesque, since there is only one log directory.</p>
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldAlterReplicaLogDirsViaAdminClient() throws Exception {
        // Given
        final String topicName = "alter-log-dirs-topic-" + UUID.randomUUID();
        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            // Discover a valid log directory from the broker
            final var nodes = adminClient.describeCluster().nodes().get();
            final var brokerIds = nodes.stream()
                .map(org.apache.kafka.common.Node::id)
                .collect(java.util.stream.Collectors.toList());
            final var logDirDescriptions = adminClient.describeLogDirs(brokerIds)
                .allDescriptions().get();
            final var logDir = logDirDescriptions.values().iterator().next()
                .keySet().iterator().next();

            // When — reassign partition 0 to the discovered log directory
            final var tpr = new org.apache.kafka.common.TopicPartitionReplica(topicName, 0, brokerIds.getFirst());
            adminClient.alterReplicaLogDirs(Map.of(tpr, logDir)).all().get();

            // Then — no exception means success
            log.info("Altered replica log dirs for topic {} to {}", topicName, logDir);
        }
    }
}
