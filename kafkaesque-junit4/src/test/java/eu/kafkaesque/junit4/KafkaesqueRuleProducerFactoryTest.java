package eu.kafkaesque.junit4;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates the producer factory methods on {@link KafkaesqueRule}.
 */
class KafkaesqueRuleProducerFactoryTest {

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
    void shouldCreateDefaultProducerThatCanSend() throws Exception {
        try (var producer = kafka.createProducer()) {
            producer.send(new ProducerRecord<>("test-topic", "key", "value")).get();
        }

        assertThat(kafka.getServer().getTotalRecordCount()).isEqualTo(1);
    }

    @Test
    void shouldCreateProducerWithCustomProperties() throws Exception {
        final var props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        try (var producer = kafka.<String, String>createProducer(props)) {
            producer.send(new ProducerRecord<>("test-topic", "key", "value")).get();
        }

        assertThat(kafka.getServer().getTotalRecordCount()).isEqualTo(1);
    }

    @Test
    void shouldAutoCloseProducerOnAfter() throws Exception {
        final var producer = kafka.createProducer();
        producer.send(new ProducerRecord<>("test-topic", "key", "value")).get();

        kafka.after();

        assertThatThrownBy(() ->
            producer.send(new ProducerRecord<>("test-topic", "key", "value")).get()
        ).isInstanceOf(IllegalStateException.class);
    }
}
