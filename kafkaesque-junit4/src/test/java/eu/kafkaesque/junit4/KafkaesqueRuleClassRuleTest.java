package eu.kafkaesque.junit4;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;

import static java.util.List.of;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Validates that {@link KafkaesqueRule} works correctly as a shared (class-scoped) rule,
 * simulated by calling {@code before()} once in {@code @BeforeAll} and {@code after()} once
 * in {@code @AfterAll}.
 */
class KafkaesqueRuleClassRuleTest {

    private static final KafkaesqueRule KAFKA = new KafkaesqueRule();

    @BeforeAll
    static void startServer() throws Throwable {
        KAFKA.before();
    }

    @AfterAll
    static void stopServer() {
        KAFKA.after();
    }

    @Test
    void shouldProvideRunningServerForProducingAndConsuming() throws Exception {
        final var topic = "test-topic-" + UUID.randomUUID();
        final var received = new ArrayList<String>();

        try (var producer = KAFKA.createProducer();
             var consumer = KAFKA.createConsumer()) {
            consumer.subscribe(of(topic));
            producer.send(new ProducerRecord<>(topic, "key1", "value1")).get();

            await().atMost(10, SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(r -> received.add(r.value()));
                return !received.isEmpty();
            });
        }

        assertThat(received).containsExactly("value1");
    }

    @Test
    void shouldReturnConsistentBootstrapServers() throws Exception {
        final var first = KAFKA.getBootstrapServers();
        final var second = KAFKA.getBootstrapServers();

        assertThat(first).isEqualTo(second);
        assertThat(first).isNotBlank();
    }
}
