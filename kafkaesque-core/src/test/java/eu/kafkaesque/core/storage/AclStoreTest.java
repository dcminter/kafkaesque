package eu.kafkaesque.core.storage;

import eu.kafkaesque.core.storage.AclStore.AclBinding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AclStore}.
 */
class AclStoreTest {

    private AclStore aclStore;

    @BeforeEach
    void setUp() {
        aclStore = new AclStore();
    }

    @Test
    void addBinding_shouldStoreBinding() {
        // Given
        final var binding = topicReadBinding("my-topic", "User:alice");

        // When
        aclStore.addBinding(binding);

        // Then
        assertThat(aclStore.getBindings()).containsExactly(binding);
    }

    @Test
    void addBinding_duplicateBinding_shouldNotCreateDuplicate() {
        // Given
        final var binding = topicReadBinding("my-topic", "User:alice");

        // When
        aclStore.addBinding(binding);
        aclStore.addBinding(binding);

        // Then
        assertThat(aclStore.getBindings()).hasSize(1);
    }

    @Test
    void getBindings_shouldReturnUnmodifiableSnapshot() {
        // Given
        final var binding = topicReadBinding("my-topic", "User:alice");
        aclStore.addBinding(binding);

        // When
        final var bindings = aclStore.getBindings();

        // Then
        assertThat(bindings).hasSize(1);
        org.junit.jupiter.api.Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> bindings.add(topicReadBinding("other", "User:bob")));
    }

    @Test
    void findMatchingBindings_withExactMatch_shouldReturnBinding() {
        // Given
        final var binding = topicReadBinding("my-topic", "User:alice");
        aclStore.addBinding(binding);

        // When
        final var matched = aclStore.findMatchingBindings(
            TOPIC_TYPE, "my-topic", LITERAL_TYPE,
            "User:alice", "*", READ_OP, ALLOW_PERM);

        // Then
        assertThat(matched).containsExactly(binding);
    }

    @Test
    void findMatchingBindings_withByteMinusOneWildcard_shouldMatchAnyResourceType() {
        // Given
        final var binding = topicReadBinding("my-topic", "User:alice");
        aclStore.addBinding(binding);

        // When — pass -1 for resourceType (wildcard)
        final var matched = aclStore.findMatchingBindings(
            (byte) -1, "my-topic", LITERAL_TYPE,
            "User:alice", "*", READ_OP, ALLOW_PERM);

        // Then
        assertThat(matched).containsExactly(binding);
    }

    @Test
    void findMatchingBindings_withAnyCode_shouldMatchAnyResourceType() {
        // Given
        final var binding = topicReadBinding("my-topic", "User:alice");
        aclStore.addBinding(binding);

        // When — pass ANY_CODE (1) for resourceType
        final var matched = aclStore.findMatchingBindings(
            ANY_CODE, "my-topic", LITERAL_TYPE,
            "User:alice", "*", READ_OP, ALLOW_PERM);

        // Then
        assertThat(matched).containsExactly(binding);
    }

    @Test
    void findMatchingBindings_withMatchCode_shouldMatchAllPatternTypes() {
        // Given
        final var binding = topicReadBinding("my-topic", "User:alice");
        aclStore.addBinding(binding);

        // When — pass MATCH_CODE (4) for resourcePatternType
        final var matched = aclStore.findMatchingBindings(
            TOPIC_TYPE, "my-topic", MATCH_CODE,
            "User:alice", "*", READ_OP, ALLOW_PERM);

        // Then
        assertThat(matched).containsExactly(binding);
    }

    @Test
    void findMatchingBindings_withNullStringFilters_shouldMatchAny() {
        // Given
        final var binding = topicReadBinding("my-topic", "User:alice");
        aclStore.addBinding(binding);

        // When — pass null for resourceName, principal, host
        final var matched = aclStore.findMatchingBindings(
            TOPIC_TYPE, null, LITERAL_TYPE,
            null, null, READ_OP, ALLOW_PERM);

        // Then
        assertThat(matched).containsExactly(binding);
    }

    @Test
    void findMatchingBindings_withNonMatchingFilter_shouldReturnEmpty() {
        // Given
        final var binding = topicReadBinding("my-topic", "User:alice");
        aclStore.addBinding(binding);

        // When — filter on a different topic name
        final var matched = aclStore.findMatchingBindings(
            TOPIC_TYPE, "other-topic", LITERAL_TYPE,
            "User:alice", "*", READ_OP, ALLOW_PERM);

        // Then
        assertThat(matched).isEmpty();
    }

    @Test
    void findMatchingBindings_multipleBindings_shouldReturnOnlyMatching() {
        // Given
        final var aliceRead = topicReadBinding("my-topic", "User:alice");
        final var bobRead = topicReadBinding("my-topic", "User:bob");
        final var aliceOther = topicReadBinding("other-topic", "User:alice");
        aclStore.addBinding(aliceRead);
        aclStore.addBinding(bobRead);
        aclStore.addBinding(aliceOther);

        // When — filter on principal=alice, resourceName=wildcard
        final var matched = aclStore.findMatchingBindings(
            TOPIC_TYPE, null, LITERAL_TYPE,
            "User:alice", "*", READ_OP, ALLOW_PERM);

        // Then
        assertThat(matched).containsExactlyInAnyOrder(aliceRead, aliceOther);
    }

    @Test
    void findMatchingBindings_withAnyCodeForOperation_shouldMatchAllOperations() {
        // Given
        final byte writeOp = 4;
        final var readBinding = topicReadBinding("my-topic", "User:alice");
        final var writeBinding = new AclBinding(
            TOPIC_TYPE, "my-topic", LITERAL_TYPE,
            "User:alice", "*", writeOp, ALLOW_PERM);
        aclStore.addBinding(readBinding);
        aclStore.addBinding(writeBinding);

        // When — pass ANY_CODE for operation
        final var matched = aclStore.findMatchingBindings(
            TOPIC_TYPE, "my-topic", LITERAL_TYPE,
            "User:alice", "*", ANY_CODE, ALLOW_PERM);

        // Then
        assertThat(matched).containsExactlyInAnyOrder(readBinding, writeBinding);
    }

    @Test
    void deleteMatchingBindings_shouldRemoveAndReturnMatched() {
        // Given
        final var binding = topicReadBinding("my-topic", "User:alice");
        aclStore.addBinding(binding);

        // When
        final var deleted = aclStore.deleteMatchingBindings(
            TOPIC_TYPE, "my-topic", LITERAL_TYPE,
            "User:alice", "*", READ_OP, ALLOW_PERM);

        // Then
        assertThat(deleted).containsExactly(binding);
        assertThat(aclStore.getBindings()).isEmpty();
    }

    @Test
    void deleteMatchingBindings_withWildcard_shouldDeleteMultiple() {
        // Given
        final var binding1 = topicReadBinding("my-topic", "User:alice");
        final var binding2 = topicReadBinding("my-topic", "User:bob");
        aclStore.addBinding(binding1);
        aclStore.addBinding(binding2);

        // When — wildcard on principal
        final var deleted = aclStore.deleteMatchingBindings(
            TOPIC_TYPE, "my-topic", LITERAL_TYPE,
            null, "*", READ_OP, ALLOW_PERM);

        // Then
        assertThat(deleted).containsExactlyInAnyOrder(binding1, binding2);
        assertThat(aclStore.getBindings()).isEmpty();
    }

    @Test
    void deleteMatchingBindings_noMatch_shouldReturnEmptyAndLeaveStoreUnchanged() {
        // Given
        final var binding = topicReadBinding("my-topic", "User:alice");
        aclStore.addBinding(binding);

        // When
        final var deleted = aclStore.deleteMatchingBindings(
            TOPIC_TYPE, "non-existent", LITERAL_TYPE,
            "User:alice", "*", READ_OP, ALLOW_PERM);

        // Then
        assertThat(deleted).isEmpty();
        assertThat(aclStore.getBindings()).hasSize(1);
    }

    // --- constants matching Kafka wire-protocol codes ---

    /** Kafka resource type code for TOPIC. */
    private static final byte TOPIC_TYPE = 2;

    /** Kafka pattern type code for LITERAL. */
    private static final byte LITERAL_TYPE = 3;

    /** Kafka ACL operation code for READ. */
    private static final byte READ_OP = 3;

    /** Kafka permission type code for ALLOW. */
    private static final byte ALLOW_PERM = 3;

    /** Kafka wire-protocol code for the ANY enum value (wildcard in filters). */
    private static final byte ANY_CODE = 1;

    /** Kafka wire-protocol code for PatternType.MATCH (wildcard in ACL filters). */
    private static final byte MATCH_CODE = 4;

    /**
     * Creates a standard TOPIC/LITERAL/READ/ALLOW binding for testing.
     *
     * @param topicName the topic resource name
     * @param principal the principal (e.g. {@code "User:alice"})
     * @return the ACL binding
     */
    private static AclBinding topicReadBinding(final String topicName, final String principal) {
        return new AclBinding(TOPIC_TYPE, topicName, LITERAL_TYPE, principal, "*", READ_OP, ALLOW_PERM);
    }
}
