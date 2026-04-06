package eu.kafkaesque.junit5;

import eu.kafkaesque.core.KafkaesqueServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import static java.util.concurrent.TimeUnit.SECONDS;

import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that {@link KafkaesqueProducer} correctly injects a configured
 * {@link KafkaProducer} into test method parameters.
 */
@Kafkaesque(topics = {
    @KafkaesqueTopic(name = "producer-inject-simple"),
    @KafkaesqueTopic(name = "producer-inject-transactional")
})
class KafkaesqueExtensionProducerInjectionTest {

    @Test
    void shouldInjectProducerThatCanSendMessages(
            final KafkaesqueServer server,
            @KafkaesqueProducer final KafkaProducer<String, String> producer) throws Exception {
        final var received = new ArrayList<String>();

        try (var consumer = createConsumer(server.getBootstrapServers())) {
            consumer.subscribe(of("producer-inject-simple"));
            producer.send(new ProducerRecord<>("producer-inject-simple", "key", "hello")).get();

            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(r -> received.add(r.value()));
                return !received.isEmpty();
            });
        }

        assertThat(received).containsExactly("hello");
    }

    @Test
    void shouldInjectTransactionalProducerThatCanCommit(
            final KafkaesqueServer server,
            @KafkaesqueProducer(transactionalId = "tx-producer-test", idempotent = true)
            final KafkaProducer<String, String> producer) throws Exception {
        final var received = new ArrayList<String>();

        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<>("producer-inject-transactional", "key", "committed")).get();
        producer.commitTransaction();

        try (var consumer = createConsumer(server.getBootstrapServers())) {
            consumer.subscribe(of("producer-inject-transactional"));
            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(r -> received.add(r.value()));
                return !received.isEmpty();
            });
        }

        assertThat(received).containsExactly("committed");
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
