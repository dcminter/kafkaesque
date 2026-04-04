package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.CleanupPolicy;
import eu.kafkaesque.core.storage.EventStore;
import eu.kafkaesque.core.storage.StoredRecord;
import eu.kafkaesque.core.storage.TopicStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static eu.kafkaesque.core.storage.CleanupPolicy.COMPACT;
import static eu.kafkaesque.core.storage.CleanupPolicy.COMPACT_DELETE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.record.CompressionType.NONE;

/**
 * Handles Kafka consumer data-plane API responses.
 *
 * <p>Covers {@link ApiKeys#LIST_OFFSETS}, {@link ApiKeys#OFFSET_FETCH},
 * {@link ApiKeys#OFFSET_COMMIT}, and {@link ApiKeys#FETCH}.</p>
 *
 * @see KafkaProtocolHandler
 * @see EventStore
 * @see GroupCoordinator
 */
@Slf4j
final class ConsumerDataApiHandler {

    private final EventStore eventStore;
    private final GroupCoordinator groupCoordinator;
    private final TopicStore topicStore;

    /**
     * Creates a new handler backed by the given event store, group coordinator, and topic store.
     *
     * @param eventStore       the store holding produced records
     * @param groupCoordinator the coordinator managing committed offsets
     * @param topicStore       the store holding topic definitions (used to resolve per-topic compression)
     */
    ConsumerDataApiHandler(
            final EventStore eventStore,
            final GroupCoordinator groupCoordinator,
            final TopicStore topicStore) {
        this.eventStore = eventStore;
        this.groupCoordinator = groupCoordinator;
        this.topicStore = topicStore;
    }

    /**
     * Generates a LIST_OFFSETS response.
     *
     * <p>Returns offset {@code 0} for {@code EARLIEST} ({@code -2}) and the current
     * high-watermark for {@code LATEST} ({@code -1}) and specific timestamps.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateListOffsetsResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new ListOffsetsRequestData(accessor, requestHeader.apiVersion());

            final var topicResponses = request.topics().stream()
                .map(topic -> new ListOffsetsResponseData.ListOffsetsTopicResponse()
                    .setName(topic.name())
                    .setPartitions(topic.partitions().stream()
                        .map(partition -> {
                            final long offset = resolveListOffset(
                                topic.name(), partition.partitionIndex(), partition.timestamp());
                            return new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                                .setPartitionIndex(partition.partitionIndex())
                                .setErrorCode((short) 0)
                                .setTimestamp(partition.timestamp() >= 0 ? partition.timestamp() : -1L)
                                .setOffset(offset)
                                .setLeaderEpoch(0);
                        })
                        .toList()))
                .toList();

            return ResponseSerializer.serialize(requestHeader,
                new ListOffsetsResponseData().setThrottleTimeMs(0).setTopics(topicResponses),
                ApiKeys.LIST_OFFSETS);

        } catch (final Exception e) {
            log.error("Error generating ListOffsets response", e);
            return null;
        }
    }

    /**
     * Generates an OFFSET_FETCH response with the committed offset for each requested partition.
     *
     * <p>Returns {@code -1} for partitions with no committed offset, causing the client to
     * apply its configured {@code auto.offset.reset} policy (earliest in the tests).</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateOffsetFetchResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new OffsetFetchRequestData(accessor, requestHeader.apiVersion());

            final var data = new OffsetFetchResponseData().setThrottleTimeMs(0);

            if (requestHeader.apiVersion() >= 8) {
                final var groupResponses = request.groups().stream()
                    .map(group -> buildOffsetFetchGroupResponse(group.groupId(), group.topics()))
                    .toList();
                data.setGroups(groupResponses);
            } else {
                data.setTopics(buildOffsetFetchTopicResponses(request.groupId(), request.topics()));
            }

            return ResponseSerializer.serialize(requestHeader, data, ApiKeys.OFFSET_FETCH);

        } catch (final Exception e) {
            log.error("Error generating OffsetFetch response", e);
            return null;
        }
    }

    /**
     * Generates an OFFSET_COMMIT response, persisting the committed offsets.
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateOffsetCommitResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new OffsetCommitRequestData(accessor, requestHeader.apiVersion());

            final var topicResponses = request.topics().stream()
                .map(topic -> new OffsetCommitResponseData.OffsetCommitResponseTopic()
                    .setName(topic.name())
                    .setPartitions(topic.partitions().stream()
                        .map(partition -> {
                            groupCoordinator.commitOffset(
                                request.groupId(), topic.name(),
                                partition.partitionIndex(), partition.committedOffset());
                            return new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(partition.partitionIndex())
                                .setErrorCode((short) 0);
                        })
                        .toList()))
                .toList();

            return ResponseSerializer.serialize(requestHeader,
                new OffsetCommitResponseData().setThrottleTimeMs(0).setTopics(topicResponses),
                ApiKeys.OFFSET_COMMIT);

        } catch (final Exception e) {
            log.error("Error generating OffsetCommit response", e);
            return null;
        }
    }

    /**
     * Generates a FETCH response containing stored records for the requested partitions.
     *
     * <p>Session tracking is not implemented; responding with {@code sessionId=0} signals
     * to the client that it should send a full (non-incremental) fetch each time.</p>
     *
     * <p>The {@code isolationLevel} field in the request is respected:</p>
     * <ul>
     *   <li>{@code 0} ({@code READ_UNCOMMITTED}) – returns all records including aborted ones,
     *       up to the high-watermark</li>
     *   <li>{@code 1} ({@code READ_COMMITTED}) – returns only non-transactional and committed
     *       records, up to the last-stable-offset (LSO)</li>
     * </ul>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateFetchResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new FetchRequestData(accessor, requestHeader.apiVersion());

            final var isolationLevel = request.isolationLevel();

            final var topicResponses = request.topics().stream()
                .map(topic -> new FetchResponseData.FetchableTopicResponse()
                    .setTopic(topic.topic())
                    .setPartitions(topic.partitions().stream()
                        .map(partition -> {
                            final var highWatermark =
                                eventStore.getRecordCount(topic.topic(), partition.partition());
                            final long lso = (isolationLevel == 1)
                                ? eventStore.getLastStableOffset(topic.topic(), partition.partition())
                                : highWatermark;
                            final var records = buildFetchRecords(
                                topic.topic(), partition.partition(),
                                partition.fetchOffset(), isolationLevel, lso);
                            return new FetchResponseData.PartitionData()
                                .setPartitionIndex(partition.partition())
                                .setErrorCode((short) 0)
                                .setHighWatermark(highWatermark)
                                .setLastStableOffset(lso)
                                .setLogStartOffset(0L)
                                .setRecords(records);
                        })
                        .toList()))
                .toList();

            final var data = new FetchResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode((short) 0)
                .setSessionId(0)
                .setResponses(topicResponses);

            return ResponseSerializer.serialize(requestHeader, data, ApiKeys.FETCH);

        } catch (final Exception e) {
            log.error("Error generating Fetch response", e);
            return null;
        }
    }

    /**
     * Resolves the offset for a LIST_OFFSETS request based on the timestamp sentinel.
     *
     * @param topic     the topic name
     * @param partition the partition index
     * @param timestamp {@code -2} for earliest, {@code -1} or specific ms for latest
     * @return the resolved offset
     */
    private long resolveListOffset(final String topic, final int partition, final long timestamp) {
        return (timestamp == -2L) ? 0L : eventStore.getRecordCount(topic, partition);
    }

    /**
     * Builds an OFFSET_FETCH group response for a single consumer group (v8+).
     *
     * @param groupId the consumer group ID
     * @param topics  the topics for which offsets are requested (v8+ plural type)
     * @return the populated group response
     */
    private OffsetFetchResponseData.OffsetFetchResponseGroup buildOffsetFetchGroupResponse(
            final String groupId,
            final List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics) {
        return new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId(groupId)
            .setTopics(buildOffsetFetchGroupTopicResponses(groupId, topics));
    }

    /**
     * Builds the list of topic responses for an OFFSET_FETCH group reply (v8+).
     *
     * @param groupId the consumer group ID
     * @param topics  the topics to fetch offsets for (v8+ plural request type)
     * @return the list of topic responses (v8+ plural response type)
     */
    private List<OffsetFetchResponseData.OffsetFetchResponseTopics> buildOffsetFetchGroupTopicResponses(
            final String groupId,
            final List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics) {

        return topics.stream()
            .map(topic -> new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName(topic.name())
                .setPartitions(topic.partitionIndexes().stream()
                    .map(partitionIndex -> {
                        final var committed =
                            groupCoordinator.getCommittedOffset(groupId, topic.name(), partitionIndex);
                        return new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                            .setPartitionIndex(partitionIndex)
                            .setCommittedOffset(committed)
                            .setCommittedLeaderEpoch(-1)
                            .setMetadata("")
                            .setErrorCode((short) 0);
                    })
                    .toList()))
            .toList();
    }

    /**
     * Builds the list of topic responses for a legacy OFFSET_FETCH reply (v0-v7).
     *
     * @param groupId the consumer group ID
     * @param topics  the topics to fetch offsets for (legacy singular request type)
     * @return the list of topic responses (legacy singular response type)
     */
    private List<OffsetFetchResponseData.OffsetFetchResponseTopic> buildOffsetFetchTopicResponses(
            final String groupId,
            final List<OffsetFetchRequestData.OffsetFetchRequestTopic> topics) {

        return topics.stream()
            .map(topic -> new OffsetFetchResponseData.OffsetFetchResponseTopic()
                .setName(topic.name())
                .setPartitions(topic.partitionIndexes().stream()
                    .map(partitionIndex -> {
                        final var committed =
                            groupCoordinator.getCommittedOffset(groupId, topic.name(), partitionIndex);
                        return new OffsetFetchResponseData.OffsetFetchResponsePartition()
                            .setPartitionIndex(partitionIndex)
                            .setCommittedOffset(committed)
                            .setCommittedLeaderEpoch(-1)
                            .setMetadata("")
                            .setErrorCode((short) 0);
                    })
                    .toList()))
            .toList();
    }

    /**
     * Builds a {@link MemoryRecords} instance from stored records at or after the given offset,
     * applying isolation-level filtering, the last-stable-offset bound, and any compaction or
     * retention policies configured on the topic.
     *
     * @param topic          the topic name
     * @param partition      the partition index
     * @param fetchOffset    the offset from which to start; records with lower offsets are skipped
     * @param isolationLevel {@code 0} for READ_UNCOMMITTED, {@code 1} for READ_COMMITTED
     * @param lso            the last-stable-offset; READ_COMMITTED records at or above this are excluded
     * @return a {@code MemoryRecords} instance, or {@link MemoryRecords#EMPTY} if none qualify
     */
    private MemoryRecords buildFetchRecords(
            final String topic, final int partition,
            final long fetchOffset, final byte isolationLevel, final long lso) {
        final var topicDef = topicStore.getTopic(topic);
        final var rawRecords = eventStore.getRecords(topic, partition, isolationLevel);
        final var effective = topicDef
            .map(def -> applyTopicPolicy(rawRecords, def))
            .orElse(rawRecords);

        // Two-stage READ_COMMITTED filtering:
        // Stage 1 (getRecords above): removes ABORTED and PENDING records server-side.
        // Stage 2 (LSO check below): additionally removes non-transactional records whose
        // offsets fall at or above the last-stable-offset, i.e. records interleaved after
        // an open-transaction record. Both stages are necessary: stage 1 alone cannot hide
        // non-transactional records that appear after a PENDING offset (the LSO boundary
        // covers them), and stage 2 alone would not remove ABORTED records below the LSO.
        // For READ_UNCOMMITTED the LSO equals the high-watermark, so stage 2 is a no-op.
        final var stored = effective.stream()
            .filter(r -> r.offset() >= fetchOffset)
            .filter(r -> isolationLevel != 1 || r.offset() < lso)
            .toList();

        if (stored.isEmpty()) {
            return MemoryRecords.EMPTY;
        }

        final var compression = topicDef
            .map(TopicStore.TopicDefinition::compression)
            .orElse(Compression.NONE);

        return buildMemoryRecords(stored, compression);
    }

    /**
     * Applies the compaction and retention policies from the given topic definition to a record list.
     *
     * @param records the full partition record list
     * @param def     the topic definition holding the active policies
     * @return the filtered record list
     */
    private List<StoredRecord> applyTopicPolicy(
            final List<StoredRecord> records, final TopicStore.TopicDefinition def) {
        var result = records;
        if (def.cleanupPolicy() == COMPACT
                || def.cleanupPolicy() == COMPACT_DELETE) {
            result = applyCompaction(result);
        }
        if (def.retentionMs() < Long.MAX_VALUE) {
            result = applyRetentionMs(result, def.retentionMs());
        }
        if (def.retentionBytes() >= 0) {
            result = applyRetentionBytes(result, def.retentionBytes());
        }
        return result;
    }

    /**
     * Applies log-compaction semantics: retains only the latest record per key.
     *
     * <p>Records with {@code null} keys are always retained unchanged.
     * Records whose latest value for a key is {@code null} (tombstones) are removed.</p>
     *
     * @param records the source record list, ordered by ascending offset
     * @return the compacted list, preserving original offset ordering
     */
    private List<StoredRecord> applyCompaction(final List<StoredRecord> records) {
        final var retainedOffsets = buildRetainedOffsetsAfterCompaction(records);
        return records.stream()
            .filter(r -> r.key() == null || retainedOffsets.contains(r.offset()))
            .toList();
    }

    /**
     * Builds the set of offsets that survive log compaction.
     *
     * <p>For each key, the record with the highest offset is the surviving candidate.
     * If that record is a tombstone (null value) it is excluded, causing the key to disappear.</p>
     *
     * @param records the source record list
     * @return the set of offsets to retain
     */
    private Set<Long> buildRetainedOffsetsAfterCompaction(final List<StoredRecord> records) {
        return records.stream()
            .filter(r -> r.key() != null)
            .collect(Collectors.toMap(
                StoredRecord::key,
                r -> r,
                (a, b) -> b.offset() > a.offset() ? b : a))
            .values().stream()
            .filter(r -> r.value() != null)
            .map(StoredRecord::offset)
            .collect(Collectors.toSet());
    }

    /**
     * Filters records whose timestamp is older than the given retention window.
     *
     * @param records     the source record list
     * @param retentionMs the maximum record age in milliseconds
     * @return records whose timestamp falls within the retention window
     */
    private List<StoredRecord> applyRetentionMs(
            final List<StoredRecord> records, final long retentionMs) {
        final long cutoffMs = System.currentTimeMillis() - retentionMs;
        return records.stream()
            .filter(r -> r.timestamp() > cutoffMs)
            .toList();
    }

    /**
     * Filters records to keep only the most-recent ones within the byte budget.
     *
     * <p>Records are included starting from the most recent (highest offset), accumulating
     * estimated byte sizes, until the running total would exceed the budget.  All older
     * records beyond that point are dropped.  The returned list preserves the original
     * ascending-offset order.</p>
     *
     * <p>Byte size is estimated as the sum of the UTF-8 encoded lengths of the record key
     * and value, treating {@code null} as zero bytes.</p>
     *
     * @param records        the source record list, ordered by ascending offset
     * @param retentionBytes the maximum total bytes to retain per partition
     * @return the filtered list, preserving ascending offset order
     */
    private List<StoredRecord> applyRetentionBytes(
            final List<StoredRecord> records, final long retentionBytes) {
        final long[] accumulated = {0L};
        return records.reversed().stream()
            .takeWhile(r -> {
                accumulated[0] += recordByteSize(r);
                return accumulated[0] <= retentionBytes;
            })
            .sorted(Comparator.comparingLong(StoredRecord::offset))
            .toList();
    }

    /**
     * Estimates the byte size of a record as the sum of the UTF-8 encoded lengths of its key
     * and value, treating {@code null} as zero bytes.
     *
     * @param record the record to measure
     * @return the estimated byte size
     */
    private static long recordByteSize(final StoredRecord record) {
        final long keyBytes = record.key() != null
            ? record.key().getBytes(UTF_8).length : 0L;
        final long valueBytes = record.value() != null
            ? record.value().getBytes(UTF_8).length : 0L;
        return keyBytes + valueBytes;
    }

    /**
     * Serialises a non-empty list of stored records into a {@link MemoryRecords} batch.
     *
     * @param stored      the records to serialise (must be non-empty)
     * @param compression the compression codec to apply
     * @return the serialised batch
     */
    private MemoryRecords buildMemoryRecords(
            final List<StoredRecord> stored, final Compression compression) {
        final long baseOffset = stored.stream()
            .mapToLong(StoredRecord::offset)
            .min()
            .orElse(0L);
        final var buf = ByteBuffer.allocate(estimateBufferSize(stored, compression));
        final var builder = MemoryRecords.builder(
            buf, RecordBatch.CURRENT_MAGIC_VALUE, compression, TimestampType.CREATE_TIME, baseOffset);
        stored.stream()
            .sorted(Comparator.comparingLong(StoredRecord::offset))
            .forEach(r -> builder.appendWithOffset(
                r.offset(),
                r.timestamp(),
                r.key() != null ? r.key().getBytes(UTF_8) : null,
                r.value() != null ? r.value().getBytes(UTF_8) : null,
                r.headers().toArray(Header[]::new)));
        return builder.build();
    }

    /**
     * Estimates the byte buffer size needed to hold the given records with the given compression.
     *
     * <p>For compressed codecs, doubles the baseline to accommodate framing overhead and
     * the possibility that small or incompressible payloads expand slightly.</p>
     *
     * @param records     the records to be written
     * @param compression the compression that will be applied
     * @return estimated buffer size in bytes
     */
    private static int estimateBufferSize(final List<StoredRecord> records, final Compression compression) {
        final int base = records.size() * 512 + 64;
        return compression.type() == NONE ? base : base * 2;
    }
}
