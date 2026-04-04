package eu.kafkaesque.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;
import java.util.stream.StreamSupport;

/**
 * Handles Kafka admin API responses.
 *
 * <p>Covers {@link ApiKeys#CREATE_TOPICS}: parses incoming topic-creation requests,
 * registers the topics in the {@link TopicStore}, and returns a success response.</p>
 *
 * @see KafkaProtocolHandler
 * @see TopicStore
 */
@Slf4j
final class AdminApiHandler {

    private final TopicStore topicStore;

    /**
     * Creates a new handler backed by the given topic store.
     *
     * @param topicStore the store that receives newly created topics
     */
    AdminApiHandler(final TopicStore topicStore) {
        this.topicStore = topicStore;
    }

    /**
     * Registers a topic in the store and builds the corresponding CREATE_TOPICS response entry.
     *
     * @param topic the topic to create
     * @return the result entry for the CREATE_TOPICS response
     */
    private CreateTopicsResponseData.CreatableTopicResult registerAndBuildTopicResult(
            final CreateTopicsRequestData.CreatableTopic topic) {
        final var name = topic.name();
        final var configs = topic.configs();
        final var config = new TopicStore.TopicCreationConfig(
            topic.numPartitions(),
            topic.replicationFactor(),
            resolveCompression(configs),
            resolveCleanupPolicy(configs),
            resolveRetentionMs(configs),
            resolveRetentionBytes(configs));
        topicStore.createTopic(name, config);
        final var topicId = topicStore.getTopic(name)
            .map(TopicStore.TopicDefinition::topicId)
            .orElse(Uuid.randomUuid());
        log.info("Created topic: name={}, partitions={}, replicationFactor={}, policy={}",
            name, config.numPartitions(), config.replicationFactor(), config.cleanupPolicy());
        return new CreateTopicsResponseData.CreatableTopicResult()
            .setName(name)
            .setTopicId(topicId)
            .setErrorCode((short) 0)
            .setErrorMessage(null)
            .setNumPartitions(config.numPartitions())
            .setReplicationFactor(config.replicationFactor());
    }

    /**
     * Resolves a {@link Compression} instance from a CREATE_TOPICS config collection.
     *
     * <p>Looks for the standard {@code compression.type} config key. Returns
     * {@link Compression#NONE} if the key is absent or unrecognised.</p>
     *
     * @param configs the config collection from the CREATE_TOPICS request
     * @return the resolved compression
     */
    private Compression resolveCompression(
            final CreateTopicsRequestData.CreatableTopicConfigCollection configs) {
        return StreamSupport.stream(configs.spliterator(), false)
            .filter(c -> "compression.type".equals(c.name()))
            .findFirst()
            .<Compression>map(c -> compressionForName(c.value()))
            .orElse(Compression.NONE);
    }

    /**
     * Resolves a {@link CleanupPolicy} from a CREATE_TOPICS config collection.
     *
     * <p>Looks for the standard {@code cleanup.policy} config key. Returns
     * {@link CleanupPolicy#DELETE} if the key is absent.</p>
     *
     * @param configs the config collection from the CREATE_TOPICS request
     * @return the resolved cleanup policy
     */
    private CleanupPolicy resolveCleanupPolicy(
            final CreateTopicsRequestData.CreatableTopicConfigCollection configs) {
        return StreamSupport.stream(configs.spliterator(), false)
            .filter(c -> "cleanup.policy".equals(c.name()))
            .findFirst()
            .map(c -> resolveCleanupPolicyValue(c.value()))
            .orElse(CleanupPolicy.DELETE);
    }

    /**
     * Maps a raw {@code cleanup.policy} config string to a {@link CleanupPolicy} constant.
     *
     * <p>Recognises {@code "compact"}, {@code "delete"}, {@code "compact,delete"}, and
     * {@code "delete,compact"} (case-sensitive, matching the values Kafka itself accepts).</p>
     *
     * @param value the raw config value
     * @return the corresponding {@link CleanupPolicy}
     */
    private static CleanupPolicy resolveCleanupPolicyValue(final String value) {
        return switch (value) {
            case "compact"        -> CleanupPolicy.COMPACT;
            case "compact,delete",
                 "delete,compact" -> CleanupPolicy.COMPACT_DELETE;
            default               -> CleanupPolicy.DELETE;
        };
    }

    /**
     * Resolves the {@code retention.ms} value from a CREATE_TOPICS config collection.
     *
     * <p>Returns {@code Long.MAX_VALUE} (unlimited) if the key is absent or unparseable.</p>
     *
     * @param configs the config collection from the CREATE_TOPICS request
     * @return the retention time in milliseconds, or {@code Long.MAX_VALUE} for unlimited
     */
    private long resolveRetentionMs(
            final CreateTopicsRequestData.CreatableTopicConfigCollection configs) {
        return StreamSupport.stream(configs.spliterator(), false)
            .filter(c -> "retention.ms".equals(c.name()))
            .findFirst()
            .map(c -> parseLongOrDefault(c.value(), Long.MAX_VALUE))
            .orElse(Long.MAX_VALUE);
    }

    /**
     * Resolves the {@code retention.bytes} value from a CREATE_TOPICS config collection.
     *
     * <p>Returns {@code -1} (unlimited) if the key is absent or unparseable.</p>
     *
     * @param configs the config collection from the CREATE_TOPICS request
     * @return the retention bytes per partition, or {@code -1} for unlimited
     */
    private long resolveRetentionBytes(
            final CreateTopicsRequestData.CreatableTopicConfigCollection configs) {
        return StreamSupport.stream(configs.spliterator(), false)
            .filter(c -> "retention.bytes".equals(c.name()))
            .findFirst()
            .map(c -> parseLongOrDefault(c.value(), -1L))
            .orElse(-1L);
    }

    /**
     * Parses a string as a {@code long}, returning a default value on failure.
     *
     * @param value        the string to parse
     * @param defaultValue the value to return if parsing fails
     * @return the parsed value or {@code defaultValue}
     */
    private static long parseLongOrDefault(final String value, final long defaultValue) {
        try {
            return Long.parseLong(value);
        } catch (final NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Maps a Kafka {@code compression.type} config value to a {@link Compression} instance.
     *
     * <p>Handles the standard broker-side values ({@code uncompressed}, {@code gzip},
     * {@code snappy}, {@code lz4}, {@code zstd}, {@code producer}).
     * {@code producer} and any unrecognised value fall back to {@link Compression#NONE}.</p>
     *
     * @param name the compression type name from the topic config
     * @return the corresponding {@link Compression}
     */
    private static Compression compressionForName(final String name) {
        return switch (name) {
            case "gzip"   -> Compression.gzip().build();
            case "snappy" -> Compression.snappy().build();
            case "lz4"    -> Compression.lz4().build();
            case "zstd"   -> Compression.zstd().build();
            default       -> Compression.NONE; // "uncompressed", "producer", unknown
        };
    }

    /**
     * Generates an INCREMENTAL_ALTER_CONFIGS success response without applying any changes.
     *
     * <p>Kafkaesque applies retention and compaction policies at fetch time rather than
     * through background processes, so broker-level config changes are no-ops. A success
     * response is returned so that clients that call this API (e.g. to tune the retention
     * check interval before a test) do not fail.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateIncrementalAlterConfigsResponse(
            final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new IncrementalAlterConfigsRequestData(accessor, requestHeader.apiVersion());
            final var responses = StreamSupport.stream(request.resources().spliterator(), false)
                .map(resource -> new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
                    .setResourceType(resource.resourceType())
                    .setResourceName(resource.resourceName())
                    .setErrorCode((short) 0)
                    .setErrorMessage(null))
                .toList();
            final var response = new IncrementalAlterConfigsResponseData()
                .setThrottleTimeMs(0)
                .setResponses(responses);
            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.INCREMENTAL_ALTER_CONFIGS);
        } catch (final Exception e) {
            log.error("Error generating IncrementalAlterConfigs response", e);
            return null;
        }
    }

    /**
     * Generates a CREATE_TOPICS response after registering the requested topics.
     *
     * <p>Each topic in the request is stored in the {@link TopicStore} and a
     * success result is returned. If a topic name already exists, the existing
     * definition is retained (idempotent behaviour).</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateCreateTopicsResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new CreateTopicsRequestData(accessor, requestHeader.apiVersion());

            final var topicResults = new CreateTopicsResponseData.CreatableTopicResultCollection();
            request.topics().stream()
                .map(this::registerAndBuildTopicResult)
                .forEach(topicResults::add);

            final var response = new CreateTopicsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(topicResults);

            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.CREATE_TOPICS);

        } catch (final Exception e) {
            log.error("Error generating CreateTopics response", e);
            return null;
        }
    }
}
