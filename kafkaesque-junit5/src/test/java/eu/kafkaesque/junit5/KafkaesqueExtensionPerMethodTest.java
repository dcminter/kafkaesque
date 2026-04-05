package eu.kafkaesque.junit5;

import eu.kafkaesque.core.KafkaesqueServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates that {@link Kafkaesque.Lifecycle#PER_METHOD} provides a fresh
 * {@link KafkaesqueServer} for each individual test method.
 */
@Kafkaesque(lifecycle = Kafkaesque.Lifecycle.PER_METHOD)
class KafkaesqueExtensionPerMethodTest {

    @Test
    void firstTestShouldStartWithNoRecords(final KafkaesqueServer server) throws Exception {
        assertThat(server.getTotalRecordCount()).isZero();

        produceRecord(server.getBootstrapServers());

        assertThat(server.getTotalRecordCount()).isEqualTo(1);
    }

    @Test
    void secondTestShouldAlsoStartWithNoRecordsFromPreviousTest(final KafkaesqueServer server) throws Exception {
        assertThat(server.getTotalRecordCount()).isZero();
    }

    private void produceRecord(final String bootstrapServers) throws Exception {
        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.send(new ProducerRecord<>("topic-" + UUID.randomUUID(), "k", "v")).get();
        }
    }
}
