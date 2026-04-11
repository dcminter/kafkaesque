package eu.kafkaesque.junit4;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates that {@link KafkaesqueRule} correctly pre-creates topics from the builder
 * configuration and honours the {@code autoCreateTopics} setting.
 */
class KafkaesqueRuleTopicCreationTest {

    @Test
    void shouldPreCreateDeclaredTopic() throws Throwable {
        final var kafka = KafkaesqueRule.builder().topic("my-topic").build();
        try {
            kafka.before();
            assertThat(kafka.getServer().getRegisteredTopics()).contains("my-topic");
        }
        finally {
            kafka.after();
        }
    }

    @Test
    void shouldPreCreateMultipleTopics() throws Throwable {
        final var kafka = KafkaesqueRule.builder()
            .topic("topic-a")
            .topics(new TopicDefinition("topic-b"), new TopicDefinition("topic-c"))
            .build();
        try {
            kafka.before();
            assertThat(kafka.getServer().getRegisteredTopics())
                .contains("topic-a", "topic-b", "topic-c");
        }
        finally {
            kafka.after();
        }
    }

    @Test
    void shouldRejectProduceToUndeclaredTopicWhenAutoCreateDisabled() throws Throwable {
        final var kafka = KafkaesqueRule.builder()
            .autoCreateTopics(false)
            .topic("declared-topic")
            .build();
        try {
            kafka.before();
            try (var producer = kafka.createProducer()) {
                assertThatThrownBy(() ->
                    producer.send(new ProducerRecord<>("unknown-topic", "key", "value")).get()
                ).isInstanceOf(ExecutionException.class);
            }
        }
        finally {
            kafka.after();
        }
    }

    @Test
    void shouldSucceedProducingToPreCreatedTopicWithAutoCreateDisabled() throws Throwable {
        final var kafka = KafkaesqueRule.builder()
            .autoCreateTopics(false)
            .topic("declared-topic")
            .build();
        try {
            kafka.before();
            try (var producer = kafka.createProducer()) {
                producer.send(new ProducerRecord<>("declared-topic", "key", "value")).get();
            }
            assertThat(kafka.getServer().getTotalRecordCount()).isEqualTo(1);
        }
        finally {
            kafka.after();
        }
    }

    @Test
    void shouldRespectCustomPartitionCount() throws Throwable {
        final var kafka = KafkaesqueRule.builder()
            .topics(new TopicDefinition("partitioned-topic", 3))
            .build();
        try {
            kafka.before();
            assertThat(kafka.getServer().getRegisteredTopics()).contains("partitioned-topic");
        }
        finally {
            kafka.after();
        }
    }
}
