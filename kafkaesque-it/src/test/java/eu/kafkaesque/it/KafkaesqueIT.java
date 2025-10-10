package eu.kafkaesque.it;

import eu.kafkaesque.core.KafkaesqueServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for Kafkaesque mock server.
 *
 * This test is a duplicate of the connection test from KafkaIntegrationTest,
 * but uses the Kafkaesque mock instead of a real Kafka instance.
 *
 * The changes from the original test are ABSOLUTELY MINIMAL - only the
 * setup of the server differs.
 */
@Slf4j
class KafkaesqueIT {
    private static final long START_TIME = System.currentTimeMillis();

    private KafkaesqueServer server;

    @BeforeEach
    void setUp() throws Exception {
        log.info("=== setUp() starting ===");
        // Start Kafkaesque mock server on an ephemeral port
        log.info("Creating KafkaesqueServer...");
        server = new KafkaesqueServer("localhost", 0);
        log.info("Starting server...");
        server.start();

        log.info("=== Kafkaesque server started on {} ===", server.getBootstrapServers());
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void shouldEstablishConnectionToKafkaesqueBroker() throws Exception {
        final long testStartTime = System.currentTimeMillis();
        log.info("Milliseconds to bootstrap: {}",(testStartTime - START_TIME));

        // Given
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server.getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        // When
        try (AdminClient adminClient = AdminClient.create(props)) {
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            String clusterId = clusterResult.clusterId().get();
            int nodeCount = clusterResult.nodes().get().size();

            // Then
            assertThat(clusterId).isNotNull();
            assertThat(nodeCount).isGreaterThan(0);

            log.info("Successfully connected to Kafkaesque cluster: id={}, nodes={}", clusterId, nodeCount);
        }
    }

    @Test
    void shouldPublishEventToKafkaesqueMock() throws Exception {
        log.info("Milliseconds to bootstrap: {}",(System.currentTimeMillis() - START_TIME));

        // Given
        final var topicName = "test-topic";
        final var messageKey = "test-key";
        final var messageValue = "Hello, Kafkaesque!";

        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        // When
        try (final var producer = new KafkaProducer<String, String>(props)) {
            final var record = new ProducerRecord<>(topicName, messageKey, messageValue);
            final Future<RecordMetadata> future = producer.send(record);
            final var metadata = future.get();

            // Then
            assertThat(metadata).isNotNull();
            assertThat(metadata.topic()).isEqualTo(topicName);
            assertThat(metadata.partition()).isGreaterThanOrEqualTo(0);
            assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);

            log.info("Successfully published message to Kafkaesque: topic={}, partition={}, offset={}, key='{}', value='{}'",
                metadata.topic(), metadata.partition(), metadata.offset(), messageKey, messageValue);
        }
    }
}
