package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.CleanupPolicy;
import eu.kafkaesque.core.storage.EventStore;
import eu.kafkaesque.core.storage.TopicStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.StreamSupport;

import static eu.kafkaesque.core.storage.CleanupPolicy.COMPACT;
import static eu.kafkaesque.core.storage.CleanupPolicy.COMPACT_DELETE;
import static eu.kafkaesque.core.storage.CleanupPolicy.DELETE;
import static java.util.stream.Collectors.toList;

/**
 * Handles Kafka admin API responses.
 *
 * <p>Covers {@link ApiKeys#CREATE_TOPICS}, {@link ApiKeys#DELETE_TOPICS},
 * and {@link ApiKeys#INCREMENTAL_ALTER_CONFIGS}: parses incoming requests,
 * updates the {@link TopicStore} and {@link EventStore} as needed, and returns
 * appropriate responses.</p>
 *
 * @see KafkaProtocolHandler
 * @see TopicStore
 * @see EventStore
 */
@Slf4j
@RequiredArgsConstructor
final class AdminApiHandler {

    private final TopicStore topicStore;
    private final EventStore eventStore;

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
        return configs.stream()
            .filter(c -> "compression.type".equals(c.name()))
            .findFirst()
            .map(c -> compressionForName(c.value()))
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
        return configs.stream()
            .filter(c -> "cleanup.policy".equals(c.name()))
            .findFirst()
            .map(c -> resolveCleanupPolicyValue(c.value()))
            .orElse(DELETE);
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
        switch (value) {
            case "compact":        return COMPACT;
            case "compact,delete":
            case "delete,compact": return COMPACT_DELETE;
            default:               return DELETE;
        }
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
        return configs.stream()
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
        return configs.stream()
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
        switch (name) {
            case "gzip":   return Compression.gzip().build();
            case "snappy": return Compression.snappy().build();
            case "lz4":    return Compression.lz4().build();
            case "zstd":   return Compression.zstd().build();
            default:       return Compression.NONE; // "uncompressed", "producer", unknown
        }
    }

    /**
     * Generates a DELETE_TOPICS response after removing the requested topics.
     *
     * <p>Each topic in the request is removed from the {@link TopicStore} and its
     * stored records are purged from the {@link EventStore}. A success result is
     * returned for every requested topic, including topics that did not exist.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateDeleteTopicsResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new DeleteTopicsRequestData(accessor, requestHeader.apiVersion());

            final var topicResults = new DeleteTopicsResponseData.DeletableTopicResultCollection();
            request.topics().stream()
                .map(topic -> {
                    final var name = topic.name();
                    topicStore.deleteTopic(name);
                    eventStore.deleteTopicData(name);
                    log.info("Deleted topic: name={}", name);
                    return new DeleteTopicsResponseData.DeletableTopicResult()
                        .setName(name)
                        .setTopicId(topic.topicId())
                        .setErrorCode((short) 0)
                        .setErrorMessage(null);
                })
                .forEach(topicResults::add);

            final var response = new DeleteTopicsResponseData()
                .setThrottleTimeMs(0)
                .setResponses(topicResults);

            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.DELETE_TOPICS);
        } catch (final Exception e) {
            log.error("Error generating DeleteTopics response", e);
            return null;
        }
    }

    /**
     * Generates a DESCRIBE_CONFIGS response for the requested resources.
     *
     * <p>For topic resources (type 2), configuration entries are derived dynamically
     * from the {@link TopicStore.TopicDefinition} fields. For broker resources (type 4),
     * a small set of hardcoded defaults is returned. Unknown resource types or
     * non-existent topics produce an empty config list with a zero error code.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateDescribeConfigsResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new DescribeConfigsRequestData(accessor, requestHeader.apiVersion());

            final var results = request.resources().stream()
                .map(this::describeResource)
                .collect(toList());

            final var response = new DescribeConfigsResponseData()
                .setThrottleTimeMs(0)
                .setResults(results);

            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.DESCRIBE_CONFIGS);
        } catch (final Exception e) {
            log.error("Error generating DescribeConfigs response", e);
            return null;
        }
    }

    /**
     * Describes the configuration for a single resource (topic or broker).
     *
     * @param resource the resource descriptor from the request
     * @return the result entry for the DESCRIBE_CONFIGS response
     */
    private DescribeConfigsResponseData.DescribeConfigsResult describeResource(
            final DescribeConfigsRequestData.DescribeConfigsResource resource) {
        final byte topicType = 2;
        final var configs = (resource.resourceType() == topicType)
            ? describeTopicConfigs(resource.resourceName())
            : List.<DescribeConfigsResponseData.DescribeConfigsResourceResult>of();

        return new DescribeConfigsResponseData.DescribeConfigsResult()
            .setErrorCode((short) 0)
            .setErrorMessage(null)
            .setResourceType(resource.resourceType())
            .setResourceName(resource.resourceName())
            .setConfigs(configs);
    }

    /**
     * Builds configuration entries for a topic from its {@link TopicStore.TopicDefinition}.
     *
     * @param topicName the topic name
     * @return the config entries, or an empty list if the topic is unknown
     */
    private List<DescribeConfigsResponseData.DescribeConfigsResourceResult> describeTopicConfigs(
            final String topicName) {
        return topicStore.getTopic(topicName)
            .map(def -> List.of(
                configEntry("cleanup.policy", def.cleanupPolicy().configValue()),
                configEntry("compression.type", compressionName(def.compression())),
                configEntry("retention.ms", String.valueOf(def.retentionMs())),
                configEntry("retention.bytes", String.valueOf(def.retentionBytes()))))
            .orElse(List.of());
    }

    /**
     * Creates a single config entry for a DESCRIBE_CONFIGS response.
     *
     * @param name  the config key name
     * @param value the config value
     * @return the config entry
     */
    private static DescribeConfigsResponseData.DescribeConfigsResourceResult configEntry(
            final String name, final String value) {
        final byte configTypeString = 2;
        final byte configSourceDefaultConfig = 5;
        return new DescribeConfigsResponseData.DescribeConfigsResourceResult()
            .setName(name)
            .setValue(value)
            .setReadOnly(false)
            .setIsDefault(false)
            .setIsSensitive(false)
            .setConfigType(configTypeString)
            .setConfigSource(configSourceDefaultConfig);
    }

    /**
     * Maps a {@link Compression} instance back to its canonical config name.
     *
     * @param compression the compression
     * @return the config value string (e.g. {@code "gzip"}, {@code "none"})
     */
    private static String compressionName(final Compression compression) {
        if (compression == null || compression == Compression.NONE) {
            return "producer";
        }
        return compression.type().name;
    }

    /**
     * Generates an ALTER_CONFIGS success response without applying any changes.
     *
     * <p>Like {@link #generateIncrementalAlterConfigsResponse}, this is a no-op.
     * Kafkaesque applies retention and compaction policies at fetch time rather than
     * through background processes.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateAlterConfigsResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new AlterConfigsRequestData(accessor, requestHeader.apiVersion());
            final var responses = request.resources().stream()
                .map(resource -> new AlterConfigsResponseData.AlterConfigsResourceResponse()
                    .setResourceType(resource.resourceType())
                    .setResourceName(resource.resourceName())
                    .setErrorCode((short) 0)
                    .setErrorMessage(null))
                .collect(toList());
            final var response = new AlterConfigsResponseData()
                .setThrottleTimeMs(0)
                .setResponses(responses);
            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.ALTER_CONFIGS);
        } catch (final Exception e) {
            log.error("Error generating AlterConfigs response", e);
            return null;
        }
    }

    /**
     * Generates a CREATE_PARTITIONS response after increasing partition counts.
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateCreatePartitionsResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new CreatePartitionsRequestData(accessor, requestHeader.apiVersion());

            final var results = request.topics().stream()
                .map(topic -> {
                    final var updated = topicStore.updatePartitionCount(topic.name(), topic.count());
                    log.info("CreatePartitions: topic={}, newCount={}, success={}", topic.name(), topic.count(), updated);
                    return new CreatePartitionsResponseData.CreatePartitionsTopicResult()
                        .setName(topic.name())
                        .setErrorCode((short) 0)
                        .setErrorMessage(null);
                })
                .collect(toList());

            final var response = new CreatePartitionsResponseData()
                .setThrottleTimeMs(0)
                .setResults(results);

            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.CREATE_PARTITIONS);
        } catch (final Exception e) {
            log.error("Error generating CreatePartitions response", e);
            return null;
        }
    }

    /**
     * Generates a DELETE_RECORDS response after advancing the log start offset for
     * the requested partitions.
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateDeleteRecordsResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new DeleteRecordsRequestData(accessor, requestHeader.apiVersion());

            final var topicResults = new DeleteRecordsResponseData.DeleteRecordsTopicResultCollection();
            for (final var topic : request.topics()) {
                final var partitionResults =
                    new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection();
                for (final var partition : topic.partitions()) {
                    final long lowWatermark = eventStore.deleteRecordsBefore(
                        topic.name(), partition.partitionIndex(), partition.offset());
                    partitionResults.add(new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                        .setPartitionIndex(partition.partitionIndex())
                        .setLowWatermark(lowWatermark)
                        .setErrorCode((short) 0));
                }
                topicResults.add(new DeleteRecordsResponseData.DeleteRecordsTopicResult()
                    .setName(topic.name())
                    .setPartitions(partitionResults));
            }

            final var response = new DeleteRecordsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(topicResults);

            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.DELETE_RECORDS);
        } catch (final Exception e) {
            log.error("Error generating DeleteRecords response", e);
            return null;
        }
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
            final var responses = request.resources().stream()
                .map(resource -> new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
                    .setResourceType(resource.resourceType())
                    .setResourceName(resource.resourceName())
                    .setErrorCode((short) 0)
                    .setErrorMessage(null))
                .collect(toList());
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

    /**
     * Generates an ELECT_LEADERS response returning success for all requested partitions.
     *
     * <p>In a single-broker mock the preferred leader is always the current leader,
     * so each partition returns {@link Errors#ELECTION_NOT_NEEDED} — matching the
     * behaviour of a real single-replica Kafka broker.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateElectLeadersResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new ElectLeadersRequestData(accessor, requestHeader.apiVersion());

            final var results = request.topicPartitions() == null
                ? List.<ElectLeadersResponseData.ReplicaElectionResult>of()
                : request.topicPartitions().stream()
                    .map(tp -> new ElectLeadersResponseData.ReplicaElectionResult()
                        .setTopic(tp.topic())
                        .setPartitionResult(tp.partitions().stream()
                            .map(p -> new ElectLeadersResponseData.PartitionResult()
                                .setPartitionId(p)
                                .setErrorCode(Errors.ELECTION_NOT_NEEDED.code())
                                .setErrorMessage(Errors.ELECTION_NOT_NEEDED.message()))
                            .collect(toList())))
                    .collect(toList());

            final var response = new ElectLeadersResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode((short) 0)
                .setReplicaElectionResults(results);

            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.ELECT_LEADERS);
        } catch (final Exception e) {
            log.error("Error generating ElectLeaders response", e);
            return null;
        }
    }

    /**
     * Generates a DESCRIBE_LOG_DIRS response with a single synthetic log directory.
     *
     * <p>Since Kafkaesque is in-memory, this returns a synthetic {@code /kafkaesque}
     * log directory containing all registered topics.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateDescribeLogDirsResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            new DescribeLogDirsRequestData(accessor, requestHeader.apiVersion());

            final var topics = topicStore.getTopics().stream()
                .map(def -> {
                    final var partitions = java.util.stream.IntStream.range(0, def.numPartitions())
                        .mapToObj(p -> new DescribeLogDirsResponseData.DescribeLogDirsPartition()
                            .setPartitionIndex(p)
                            .setPartitionSize(0L)
                            .setOffsetLag(0L)
                            .setIsFutureKey(false))
                        .collect(toList());
                    return new DescribeLogDirsResponseData.DescribeLogDirsTopic()
                        .setName(def.name())
                        .setPartitions(partitions);
                })
                .collect(toList());

            final var logDir = new DescribeLogDirsResponseData.DescribeLogDirsResult()
                .setErrorCode((short) 0)
                .setLogDir("/kafkaesque")
                .setTopics(topics)
                .setTotalBytes(-1L)
                .setUsableBytes(-1L);

            final var response = new DescribeLogDirsResponseData()
                .setThrottleTimeMs(0)
                .setResults(List.of(logDir));

            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.DESCRIBE_LOG_DIRS);
        } catch (final Exception e) {
            log.error("Error generating DescribeLogDirs response", e);
            return null;
        }
    }

    /**
     * Generates an ALTER_REPLICA_LOG_DIRS response returning success for all entries.
     *
     * <p>Since Kafkaesque is in-memory with a single log directory, this is a no-op.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateAlterReplicaLogDirsResponse(
            final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new AlterReplicaLogDirsRequestData(accessor, requestHeader.apiVersion());

            final var results = request.dirs().stream()
                .flatMap(dir -> dir.topics().stream()
                    .map(topic -> new AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult()
                        .setTopicName(topic.name())
                        .setPartitions(topic.partitions().stream()
                            .map(p -> new AlterReplicaLogDirsResponseData
                                .AlterReplicaLogDirPartitionResult()
                                .setPartitionIndex(p)
                                .setErrorCode((short) 0))
                            .collect(toList()))))
                .collect(toList());

            final var response = new AlterReplicaLogDirsResponseData()
                .setThrottleTimeMs(0)
                .setResults(results);

            return ResponseSerializer.serialize(
                requestHeader, response, ApiKeys.ALTER_REPLICA_LOG_DIRS);
        } catch (final Exception e) {
            log.error("Error generating AlterReplicaLogDirs response", e);
            return null;
        }
    }
}
