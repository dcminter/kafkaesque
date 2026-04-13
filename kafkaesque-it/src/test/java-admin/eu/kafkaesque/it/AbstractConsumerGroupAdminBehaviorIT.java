package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static eu.kafkaesque.it.KafkaCompat.poll;
import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract base class defining the consumer-group admin integration test suite.
 *
 * <p>Covers listing, describing, and deleting consumer groups via
 * {@link AdminClient}. Subclasses must implement {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractConsumerGroupAdminBehaviorIT {

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Verifies that a consumer group appears in the admin listing after a consumer
     * has subscribed and polled.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldListConsumerGroupsViaAdminClient() throws Exception {
        // Given
        final String topicName = "list-groups-topic-" + UUID.randomUUID();
        final String groupId = "list-groups-group-" + UUID.randomUUID();
        final var adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            // Produce a record so the consumer has something to poll
            try (final var producer = KafkaTestClientFactory.createProducer(getBootstrapServers())) {
                producer.send(new ProducerRecord<>(topicName, "key", "value")).get();
            }

            // Create and poll a consumer to establish the group
            try (final var consumer = createConsumer(getBootstrapServers(), groupId)) {
                consumer.subscribe(of(topicName));
                poll(consumer, 10000);
                consumer.commitSync();

                // When
                final Set<String> groupIds = adminClient.listConsumerGroups().all().get().stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toSet());

                // Then
                assertThat(groupIds).contains(groupId);
                log.info("Listed consumer groups, found: {}", groupId);
            }
        }
    }

    /**
     * Verifies that a consumer group can be described via the admin API and returns
     * the expected member count.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldDescribeConsumerGroupViaAdminClient() throws Exception {
        // Given
        final String topicName = "describe-group-topic-" + UUID.randomUUID();
        final String groupId = "describe-group-" + UUID.randomUUID();
        final var adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            try (final var producer = KafkaTestClientFactory.createProducer(getBootstrapServers())) {
                producer.send(new ProducerRecord<>(topicName, "key", "value")).get();
            }

            try (final var consumer = createConsumer(getBootstrapServers(), groupId)) {
                consumer.subscribe(of(topicName));
                poll(consumer, 10000);
                consumer.commitSync();

                // When
                final var descriptions = adminClient.describeConsumerGroups(of(groupId)).all().get();

                // Then
                assertThat(descriptions).containsKey(groupId);
                final var description = descriptions.get(groupId);
                assertThat(description.members()).hasSize(1);
                log.info("Described group {}: {} member(s)", groupId, description.members().size());
            }
        }
    }

    /**
     * Verifies that a consumer group can be deleted via the admin API after the
     * consumer has been closed.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldDeleteConsumerGroupViaAdminClient() throws Exception {
        // Given
        final String topicName = "delete-group-topic-" + UUID.randomUUID();
        final String groupId = "delete-group-" + UUID.randomUUID();
        final var adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            try (final var producer = KafkaTestClientFactory.createProducer(getBootstrapServers())) {
                producer.send(new ProducerRecord<>(topicName, "key", "value")).get();
            }

            // Create consumer, poll, commit, then close it
            try (final var consumer = createConsumer(getBootstrapServers(), groupId)) {
                consumer.subscribe(of(topicName));
                poll(consumer, 10000);
                consumer.commitSync();
            }

            // When — delete the now-empty group
            adminClient.deleteConsumerGroups(of(groupId)).all().get();

            // Then
            final Set<String> remaining = adminClient.listConsumerGroups().all().get().stream()
                .map(ConsumerGroupListing::groupId)
                .collect(Collectors.toSet());
            assertThat(remaining).doesNotContain(groupId);
            log.info("Deleted consumer group: {}", groupId);
        }
    }

    /**
     * Verifies that committed consumer group offsets can be listed via the admin API.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldListConsumerGroupOffsetsViaAdminClient() throws Exception {
        // Given
        final String topicName = "list-offsets-topic-" + UUID.randomUUID();
        final String groupId = "list-offsets-group-" + UUID.randomUUID();
        final var adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            try (final var producer = KafkaTestClientFactory.createProducer(getBootstrapServers())) {
                producer.send(new ProducerRecord<>(topicName, "key", "value")).get();
            }

            try (final var consumer = createConsumer(getBootstrapServers(), groupId)) {
                consumer.subscribe(of(topicName));
                poll(consumer, 10000);
                consumer.commitSync();
            }

            // When
            final var offsets = adminClient.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get();

            // Then
            assertThat(offsets).isNotEmpty();
            final var tp = new org.apache.kafka.common.TopicPartition(topicName, 0);
            assertThat(offsets).containsKey(tp);
            assertThat(offsets.get(tp).offset()).isGreaterThanOrEqualTo(1);

            log.info("Listed offsets for group {}: {}", groupId, offsets);
        }
    }

    /**
     * Creates a consumer with the given group ID.
     *
     * @param bootstrapServers the broker address
     * @param groupId          the consumer group ID
     * @return a new consumer instance
     */
    private static KafkaConsumer<String, String> createConsumer(
            final String bootstrapServers, final String groupId) {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }
}
