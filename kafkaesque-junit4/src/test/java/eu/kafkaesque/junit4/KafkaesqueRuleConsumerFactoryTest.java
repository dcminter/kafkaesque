package eu.kafkaesque.junit4;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Properties;

import static java.util.List.of;
import static java.util.concurrent.TimeUnit.SECONDS;
import static eu.kafkaesque.junit4.KafkaCompat.poll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Validates the consumer factory methods on {@link KafkaesqueRule}.
 */
class KafkaesqueRuleConsumerFactoryTest {

    private final KafkaesqueRule kafka = new KafkaesqueRule();

    @BeforeEach
    void startServer() throws Throwable {
        kafka.before();
    }

    @AfterEach
    void stopServer() {
        kafka.after();
    }

    @Test
    void shouldCreateDefaultConsumerWithManualSubscription() throws Exception {
        final var received = new ArrayList<String>();

        try (var producer = kafka.createProducer();
             var consumer = kafka.createConsumer()) {
            consumer.subscribe(of("test-topic"));
            producer.send(new ProducerRecord<>("test-topic", "key", "value")).get();

            await().atMost(10, SECONDS).until(() -> {
                poll(consumer, 100).forEach(r -> received.add(r.value()));
                return !received.isEmpty();
            });
        }

        assertThat(received).containsExactly("value");
    }

    @Test
    void shouldCreateConsumerPreSubscribedToTopics() throws Exception {
        final var received = new ArrayList<String>();

        try (var producer = kafka.createProducer();
             var consumer = kafka.createConsumer("test-topic")) {
            producer.send(new ProducerRecord<>("test-topic", "key", "value")).get();

            await().atMost(10, SECONDS).until(() -> {
                poll(consumer, 100).forEach(r -> received.add(r.value()));
                return !received.isEmpty();
            });
        }

        assertThat(received).containsExactly("value");
    }

    @Test
    void shouldCreateConsumerWithCustomDeserializers() throws Exception {
        final var received = new ArrayList<String>();

        try (var producer = kafka.createProducer();
             var consumer = kafka.createConsumer(
                 StringDeserializer.class, StringDeserializer.class, "test-topic")) {
            producer.send(new ProducerRecord<>("test-topic", "key", "value")).get();

            await().atMost(10, SECONDS).until(() -> {
                poll(consumer, 100).forEach(r -> received.add(r.value()));
                return !received.isEmpty();
            });
        }

        assertThat(received).containsExactly("value");
    }

    @Test
    void shouldCreateConsumerWithCustomProperties() throws Exception {
        final var received = new ArrayList<String>();
        final var props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "custom-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var producer = kafka.createProducer();
             var consumer = kafka.<String, String>createConsumer(props)) {
            consumer.subscribe(of("test-topic"));
            producer.send(new ProducerRecord<>("test-topic", "key", "value")).get();

            await().atMost(10, SECONDS).until(() -> {
                poll(consumer, 100).forEach(r -> received.add(r.value()));
                return !received.isEmpty();
            });
        }

        assertThat(received).containsExactly("value");
    }

    @Test
    void shouldAutoCloseConsumerOnAfter() throws Exception {
        final var consumer = kafka.createConsumer("test-topic");

        kafka.after();

        assertThatThrownBy(() -> poll(consumer, 100))
            .isInstanceOf(IllegalStateException.class);
    }
}
