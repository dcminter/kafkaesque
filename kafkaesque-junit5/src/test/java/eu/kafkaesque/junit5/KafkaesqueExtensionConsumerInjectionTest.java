package eu.kafkaesque.junit5;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import static java.util.concurrent.TimeUnit.SECONDS;

import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that {@link KafkaesqueConsumer} correctly injects a configured
 * {@link KafkaConsumer} into test method parameters, including auto-subscription
 * and isolation level configuration.
 */
@Kafkaesque(lifecycle = Kafkaesque.Lifecycle.PER_METHOD, topics = @KafkaesqueTopic(name = "consumer-injection-topic"))
class KafkaesqueExtensionConsumerInjectionTest {

    @Test
    void shouldInjectConsumerAutoSubscribedToTopics(
            @KafkaesqueProducer final KafkaProducer<String, String> producer,
            @KafkaesqueConsumer(topics = "consumer-injection-topic")
            final KafkaConsumer<String, String> consumer) throws Exception {
        final var received = new ArrayList<String>();

        producer.send(new ProducerRecord<>("consumer-injection-topic", "key", "world")).get();

        await().atMost(10, SECONDS).until(() -> {
            consumer.poll(Duration.ofMillis(100)).forEach(r -> received.add(r.value()));
            return !received.isEmpty();
        });

        assertThat(received).containsExactly("world");
    }

    @Test
    void shouldInjectConsumerWithoutAutoSubscribeWhenNoTopicsGiven(
            @KafkaesqueProducer final KafkaProducer<String, String> producer,
            @KafkaesqueConsumer final KafkaConsumer<String, String> consumer) throws Exception {
        final var received = new ArrayList<String>();

        consumer.subscribe(of("consumer-injection-topic"));
        producer.send(new ProducerRecord<>("consumer-injection-topic", "key", "manual")).get();

        await().atMost(10, SECONDS).until(() -> {
            consumer.poll(Duration.ofMillis(100)).forEach(r -> received.add(r.value()));
            return !received.isEmpty();
        });

        assertThat(received).containsExactly("manual");
    }

    @Test
    void shouldInjectReadCommittedConsumerThatOnlySeesCommittedMessages(
            @KafkaesqueProducer(transactionalId = "tx-consumer-test", idempotent = true)
            final KafkaProducer<String, String> producer,
            @KafkaesqueConsumer(
                topics = "consumer-injection-topic",
                isolationLevel = IsolationLevel.READ_COMMITTED)
            final KafkaConsumer<String, String> consumer) throws Exception {
        final var received = new ArrayList<String>();

        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<>("consumer-injection-topic", "k", "committed")).get();
        producer.commitTransaction();

        await().atMost(10, SECONDS).until(() -> {
            consumer.poll(Duration.ofMillis(100)).forEach(r -> received.add(r.value()));
            return !received.isEmpty();
        });

        assertThat(received).containsExactly("committed");
    }
}
