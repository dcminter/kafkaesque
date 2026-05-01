package eu.kafkaesque.core.handler;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link KafkaProtocolHandler} registers a dispatcher entry for every API key
 * the project intends to support.
 *
 * <p>The map-based dispatcher cannot rely on the compiler to flag a missing case the way the
 * earlier {@code switch} statement could, so this test pins the expected set explicitly. If a
 * contributor adds a new {@code generate*Response} method on a collaborator without registering
 * it in {@link KafkaProtocolHandler}, or registers a key the project did not intend to support,
 * this test fails.</p>
 *
 * <p>Note: API keys that Kafkaesque does <em>not</em> implement (notably the telemetry APIs
 * {@code GET_TELEMETRY_SUBSCRIPTIONS} and {@code PUSH_TELEMETRY}) are deliberately absent
 * from this set. They flow through the dispatcher's unhandled-key path, which now returns a
 * properly-shaped {@code UNSUPPORTED_VERSION} response built via the parsed request's
 * {@code getErrorResponse}.</p>
 */
class KafkaProtocolHandlerDispatchTest {

    /**
     * The complete set of {@link ApiKeys} values the dispatcher is expected to handle.
     *
     * <p>Update this set deliberately when supported APIs change, in lockstep with the
     * registration code in {@link KafkaProtocolHandler#buildHandlers()}.</p>
     */
    private static final Set<ApiKeys> EXPECTED_API_KEYS = EnumSet.of(
        ApiKeys.API_VERSIONS,
        ApiKeys.METADATA,
        ApiKeys.DESCRIBE_CLUSTER,
        ApiKeys.FIND_COORDINATOR,
        ApiKeys.DESCRIBE_TOPIC_PARTITIONS,
        ApiKeys.PRODUCE,
        ApiKeys.JOIN_GROUP,
        ApiKeys.SYNC_GROUP,
        ApiKeys.HEARTBEAT,
        ApiKeys.LEAVE_GROUP,
        ApiKeys.LIST_GROUPS,
        ApiKeys.CONSUMER_GROUP_DESCRIBE,
        ApiKeys.DELETE_GROUPS,
        ApiKeys.DESCRIBE_GROUPS,
        ApiKeys.OFFSET_FETCH,
        ApiKeys.OFFSET_COMMIT,
        ApiKeys.LIST_OFFSETS,
        ApiKeys.FETCH,
        ApiKeys.CREATE_TOPICS,
        ApiKeys.DELETE_TOPICS,
        ApiKeys.DESCRIBE_CONFIGS,
        ApiKeys.ALTER_CONFIGS,
        ApiKeys.CREATE_PARTITIONS,
        ApiKeys.DELETE_RECORDS,
        ApiKeys.INCREMENTAL_ALTER_CONFIGS,
        ApiKeys.ELECT_LEADERS,
        ApiKeys.DESCRIBE_LOG_DIRS,
        ApiKeys.ALTER_REPLICA_LOG_DIRS,
        ApiKeys.CREATE_ACLS,
        ApiKeys.DESCRIBE_ACLS,
        ApiKeys.DELETE_ACLS,
        ApiKeys.INIT_PRODUCER_ID,
        ApiKeys.ADD_PARTITIONS_TO_TXN,
        ApiKeys.ADD_OFFSETS_TO_TXN,
        ApiKeys.END_TXN,
        ApiKeys.WRITE_TXN_MARKERS,
        ApiKeys.TXN_OFFSET_COMMIT,
        ApiKeys.LIST_TRANSACTIONS,
        ApiKeys.DESCRIBE_TRANSACTIONS
    );

    /**
     * Asserts the dispatcher registers exactly the expected set of API keys — no more, no fewer.
     */
    @Test
    void shouldRegisterExpectedApiKeys() {
        final var handler = new KafkaProtocolHandler();
        try {
            assertThat(handler.registeredApiKeys()).isEqualTo(EXPECTED_API_KEYS);
        } finally {
            handler.close();
        }
    }
}
