package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Kafka behavior.
 *
 * These tests currently run against a real Kafka instance using Testcontainers.
 * In the future, they will be pointed at the Kafkaesque mock implementation to verify
 * that it provides the same behavior as a real Kafka service.
 */
@Testcontainers
@Slf4j
class KafkaIT {
    private static final String TOPIC_NAME = "test-topic";
    private static final long START_TIME = System.currentTimeMillis();

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.8.0")
    );

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private String topicName;

    @BeforeEach
    void setUp() {
        topicName = TOPIC_NAME + "-" + UUID.randomUUID();
        producer = createProducer();
        consumer = createConsumer();
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void shouldEstablishConnectionToKafkaBroker() throws Exception {
        log.info("Milliseconds to bootstrap: {}",(System.currentTimeMillis() - START_TIME));

        // Given
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        // When
        try (AdminClient adminClient = AdminClient.create(props)) {
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            String clusterId = clusterResult.clusterId().get();
            int nodeCount = clusterResult.nodes().get().size();

            // Then
            assertThat(clusterId).isNotNull();
            assertThat(nodeCount).isGreaterThan(0);

            log.info("Successfully connected to Kafka cluster: id={}, nodes={}", clusterId, nodeCount);
        }
    }

    @Test
    void shouldPublishEventsSynchronouslyToKafka() throws ExecutionException, InterruptedException {
        log.info("Milliseconds to bootstrap: {}",(System.currentTimeMillis() - START_TIME));

        // Given
        String key = "test-key";
        String value = "test-value";
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

        // When
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get(); // Synchronous send

        // Then
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(topicName);
        assertThat(metadata.partition()).isGreaterThanOrEqualTo(0);
        assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);

        log.info("Successfully published message to topic={}, partition={}, offset={}",
                 metadata.topic(), metadata.partition(), metadata.offset());
    }

    @Test
    void shouldPublishMultipleEventsSynchronously() throws ExecutionException, InterruptedException {
        log.info("Milliseconds to bootstrap: {}",(System.currentTimeMillis() - START_TIME));

        // Given
        int messageCount = 5;
        List<RecordMetadata> metadataList = new ArrayList<>();

        // When
        for (int i = 0; i < messageCount; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                topicName,
                "key-" + i,
                "value-" + i
            );
            RecordMetadata metadata = producer.send(record).get();
            metadataList.add(metadata);
        }

        // Then
        assertThat(metadataList).hasSize(messageCount);
        assertThat(metadataList).allSatisfy(metadata -> {
            assertThat(metadata.topic()).isEqualTo(topicName);
            assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);
        });

        log.info("Successfully published {} messages", messageCount);
    }

    @Test
    void shouldReceivePublishedEventsFromKafka() {
        log.info("Milliseconds to bootstrap: {}",(System.currentTimeMillis() - START_TIME));

        // Given - publish some messages first
        String key1 = "key-1";
        String value1 = "value-1";
        String key2 = "key-2";
        String value2 = "value-2";

        producer.send(new ProducerRecord<>(topicName, key1, value1));
        producer.send(new ProducerRecord<>(topicName, key2, value2));
        producer.flush();

        // When - consume messages
        consumer.subscribe(List.of(topicName));

        List<ConsumerRecord<String, String>> receivedRecords = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        long timeout = 10_000; // 10 seconds

        while (receivedRecords.size() < 2 && (System.currentTimeMillis() - startTime) < timeout) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(receivedRecords::add);
        }

        // Then
        assertThat(receivedRecords).hasSize(2);

        assertThat(receivedRecords.get(0).key()).isEqualTo(key1);
        assertThat(receivedRecords.get(0).value()).isEqualTo(value1);
        assertThat(receivedRecords.get(0).topic()).isEqualTo(topicName);

        assertThat(receivedRecords.get(1).key()).isEqualTo(key2);
        assertThat(receivedRecords.get(1).value()).isEqualTo(value2);
        assertThat(receivedRecords.get(1).topic()).isEqualTo(topicName);

        log.info("Successfully consumed {} messages", receivedRecords.size());
    }

    @Test
    void shouldMaintainMessageOrderWithinPartition() throws ExecutionException, InterruptedException {
        log.info("Milliseconds to bootstrap: {}",(System.currentTimeMillis() - START_TIME));

        // Given - messages with the same key will go to the same partition
        String commonKey = "same-key";
        int messageCount = 10;

        // When - publish messages
        for (int i = 0; i < messageCount; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                topicName,
                commonKey,
                "message-" + i
            );
            producer.send(record).get(); // Synchronous to ensure order
        }

        // Then - consume and verify order
        consumer.subscribe(List.of(topicName));

        List<String> receivedValues = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        long timeout = 10_000;

        while (receivedValues.size() < messageCount && (System.currentTimeMillis() - startTime) < timeout) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> receivedValues.add(record.value()));
        }

        assertThat(receivedValues).hasSize(messageCount);
        for (int i = 0; i < messageCount; i++) {
            assertThat(receivedValues.get(i)).isEqualTo("message-" + i);
        }

        log.info("Successfully verified message ordering for {} messages", messageCount);
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        return new KafkaConsumer<>(props);
    }
}
