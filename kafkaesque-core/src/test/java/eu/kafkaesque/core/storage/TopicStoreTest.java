package eu.kafkaesque.core.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TopicStore}.
 */
class TopicStoreTest {

    private TopicStore topicStore;

    @BeforeEach
    void setUp() {
        topicStore = new TopicStore();
    }

    @Test
    void shouldReturnFalseForUnknownTopic() {
        assertThat(topicStore.hasTopic("unknown")).isFalse();
    }

    @Test
    void shouldReturnTrueAfterTopicCreated() {
        topicStore.createTopic("my-topic", 3, (short) 1);
        assertThat(topicStore.hasTopic("my-topic")).isTrue();
    }

    @Test
    void shouldReturnEmptyOptionalForUnknownTopic() {
        assertThat(topicStore.getTopic("unknown")).isEmpty();
    }

    @Test
    void shouldReturnDefinitionAfterTopicCreated() {
        topicStore.createTopic("my-topic", 3, (short) 2);

        assertThat(topicStore.getTopic("my-topic")).hasValueSatisfying(def -> {
            assertThat(def.name()).isEqualTo("my-topic");
            assertThat(def.numPartitions()).isEqualTo(3);
            assertThat(def.replicationFactor()).isEqualTo((short) 2);
        });
    }

    @Test
    void shouldRetainExistingDefinitionOnDuplicateCreate() {
        topicStore.createTopic("my-topic", 3, (short) 1);
        topicStore.createTopic("my-topic", 10, (short) 3);

        assertThat(topicStore.getTopic("my-topic")).hasValueSatisfying(def ->
            assertThat(def.numPartitions()).isEqualTo(3));
    }

    @Test
    void shouldReturnAllCreatedTopics() {
        topicStore.createTopic("topic-a", 1, (short) 1);
        topicStore.createTopic("topic-b", 2, (short) 1);

        assertThat(topicStore.getTopics())
            .extracting(TopicStore.TopicDefinition::name)
            .containsExactlyInAnyOrder("topic-a", "topic-b");
    }

    @Test
    void shouldReturnEmptyCollectionWhenNoTopicsCreated() {
        assertThat(topicStore.getTopics()).isEmpty();
    }

    @Test
    void shouldUpdatePartitionCountWhenNewCountIsGreater() {
        topicStore.createTopic("my-topic", 3, (short) 1);
        final var originalId = topicStore.getTopic("my-topic").orElseThrow().topicId();

        final var result = topicStore.updatePartitionCount("my-topic", 6);

        assertThat(result).isTrue();
        assertThat(topicStore.getTopic("my-topic")).hasValueSatisfying(def -> {
            assertThat(def.numPartitions()).isEqualTo(6);
            assertThat(def.topicId()).isEqualTo(originalId);
        });
    }

    @Test
    void shouldReturnFalseWhenUpdatingPartitionCountToSameValue() {
        topicStore.createTopic("my-topic", 3, (short) 1);

        assertThat(topicStore.updatePartitionCount("my-topic", 3)).isFalse();
        assertThat(topicStore.getTopic("my-topic")).hasValueSatisfying(def ->
            assertThat(def.numPartitions()).isEqualTo(3));
    }

    @Test
    void shouldReturnFalseWhenDecreasingPartitionCount() {
        topicStore.createTopic("my-topic", 5, (short) 1);

        assertThat(topicStore.updatePartitionCount("my-topic", 2)).isFalse();
        assertThat(topicStore.getTopic("my-topic")).hasValueSatisfying(def ->
            assertThat(def.numPartitions()).isEqualTo(5));
    }

    @Test
    void shouldReturnFalseWhenUpdatingPartitionCountForNonExistentTopic() {
        assertThat(topicStore.updatePartitionCount("ghost", 5)).isFalse();
    }

    @Test
    void shouldDeleteExistingTopicAndReturnTrue() {
        topicStore.createTopic("my-topic", 3, (short) 1);

        assertThat(topicStore.deleteTopic("my-topic")).isTrue();
        assertThat(topicStore.hasTopic("my-topic")).isFalse();
        assertThat(topicStore.deleteTopic("my-topic")).isFalse();
    }
}
