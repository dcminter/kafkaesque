package eu.kafkaesque.junit4;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates that {@link KafkaesqueRule} provides a fresh server for each test method when
 * used as a per-method rule (calling {@code before()}/{@code after()} in
 * {@code @BeforeEach}/{@code @AfterEach}).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaesqueRulePerMethodTest {

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
    @Order(1)
    void firstTestShouldStartWithNoRecords() throws Exception {
        try (var producer = kafka.createProducer()) {
            producer.send(new ProducerRecord<>("topic", "key", "value")).get();
        }
        assertThat(kafka.getServer().getTotalRecordCount()).isEqualTo(1);
    }

    @Test
    @Order(2)
    void secondTestShouldAlsoStartWithNoRecords() {
        assertThat(kafka.getServer().getTotalRecordCount()).isZero();
    }
}
