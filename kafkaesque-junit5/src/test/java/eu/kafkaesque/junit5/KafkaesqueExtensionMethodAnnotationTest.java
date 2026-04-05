package eu.kafkaesque.junit5;

import eu.kafkaesque.core.KafkaesqueServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates that {@link Kafkaesque} applied to an individual test method provides a dedicated
 * {@link KafkaesqueServer} instance for that method.
 */
class KafkaesqueExtensionMethodAnnotationTest {

    /**
     * When no class-level annotation is present, a method annotated with {@link Kafkaesque}
     * should still receive a functional server scoped to that method.
     */
    @Nested
    class WhenClassHasNoAnnotation {

        @Kafkaesque
        @Test
        void annotatedMethodReceivesAFunctionalServer(final KafkaesqueServer server) throws Exception {
            assertThat(server.getTotalRecordCount()).isZero();

            produceRecord(server.getBootstrapServers());

            assertThat(server.getTotalRecordCount()).isEqualTo(1);
        }
    }

    /**
     * When the class carries {@link Kafkaesque} (PER_CLASS), a method that also carries
     * {@link Kafkaesque} should receive its own fresh server rather than the shared class server.
     */
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @Kafkaesque
    @Nested
    class WhenClassHasPerClassAnnotation {

        @Order(1)
        @Test
        void classServerAccumulatesRecords(final KafkaesqueServer server) throws Exception {
            produceRecord(server.getBootstrapServers());
            assertThat(server.getTotalRecordCount()).isEqualTo(1);
        }

        @Order(2)
        @Kafkaesque
        @Test
        void methodAnnotationReceivesItsOwnFreshServer(final KafkaesqueServer server) throws Exception {
            // If this were the shared class server it would already have the record from @Order(1).
            assertThat(server.getTotalRecordCount()).isZero();
        }
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
