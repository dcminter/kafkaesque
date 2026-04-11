package eu.kafkaesque.junit4;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the convenience constructors of {@link TopicDefinition}.
 */
class TopicDefinitionTest {

    @Test
    void singleArgConstructorShouldDefaultToOnePartitionAndReplicationFactorOne() {
        final var topic = new TopicDefinition("my-topic");

        assertThat(topic.name()).isEqualTo("my-topic");
        assertThat(topic.numPartitions()).isEqualTo(1);
        assertThat(topic.replicationFactor()).isEqualTo((short) 1);
    }

    @Test
    void twoArgConstructorShouldDefaultReplicationFactorToOne() {
        final var topic = new TopicDefinition("my-topic", 5);

        assertThat(topic.name()).isEqualTo("my-topic");
        assertThat(topic.numPartitions()).isEqualTo(5);
        assertThat(topic.replicationFactor()).isEqualTo((short) 1);
    }

    @Test
    void canonicalConstructorShouldPreserveAllValues() {
        final var topic = new TopicDefinition("my-topic", 3, (short) 2);

        assertThat(topic.name()).isEqualTo("my-topic");
        assertThat(topic.numPartitions()).isEqualTo(3);
        assertThat(topic.replicationFactor()).isEqualTo((short) 2);
    }
}
