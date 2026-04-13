package eu.kafkaesque.junit5;

import eu.kafkaesque.core.KafkaesqueServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import static java.util.concurrent.TimeUnit.SECONDS;

import static eu.kafkaesque.junit5.KafkaCompat.poll;
import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Validates that the {@link Kafkaesque} annotation correctly provides a running
 * {@link KafkaesqueServer} that enables real Kafka producer and consumer operations.
 */
@Kafkaesque
class KafkaesqueExtensionTest {

    @Test
    void shouldProvideKafkaesqueServerForProducingAndConsuming(final KafkaesqueServer server) throws Exception {
        final var bootstrapServers = server.getBootstrapServers();
        final var topic = "test-topic-" + UUID.randomUUID();
        final var received = new ArrayList<String>();

        try (var producer = createProducer(bootstrapServers);
             var consumer = createConsumer(bootstrapServers)) {
            consumer.subscribe(of(topic));
            producer.send(new ProducerRecord<>(topic, "key1", "value1")).get();

            await().atMost(10, SECONDS).until(() -> {
                poll(consumer, 100).forEach(r -> received.add(r.value()));
                return !received.isEmpty();
            });
        }

        assertThat(received).containsExactly("value1");
    }

    private KafkaProducer<String, String> createProducer(final String bootstrapServers) {
        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createConsumer(final String bootstrapServers) {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(props);
    }
}
