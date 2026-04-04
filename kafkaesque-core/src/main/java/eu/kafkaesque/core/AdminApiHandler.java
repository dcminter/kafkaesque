package eu.kafkaesque.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
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
        final var numPartitions = topic.numPartitions();
        final var replicationFactor = topic.replicationFactor();
        final var compression = resolveCompression(topic.configs());
        topicStore.createTopic(name, numPartitions, replicationFactor, compression);
        final var topicId = topicStore.getTopic(name)
            .map(TopicStore.TopicDefinition::topicId)
            .orElse(Uuid.randomUuid());
        log.info("Created topic: name={}, partitions={}, replicationFactor={}",
            name, numPartitions, replicationFactor);
        return new CreateTopicsResponseData.CreatableTopicResult()
            .setName(name)
            .setTopicId(topicId)
            .setErrorCode((short) 0)
            .setErrorMessage(null)
            .setNumPartitions(numPartitions)
            .setReplicationFactor(replicationFactor);
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
