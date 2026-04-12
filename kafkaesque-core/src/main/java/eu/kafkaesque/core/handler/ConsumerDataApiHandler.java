package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.handler.FetchSessionCoordinator.FetchSession;
import eu.kafkaesque.core.handler.FetchSessionCoordinator.PartitionFetchState;
import eu.kafkaesque.core.handler.FetchSessionCoordinator.TopicPartitionKey;
import eu.kafkaesque.core.storage.CleanupPolicy;
import eu.kafkaesque.core.storage.EventStore;
import eu.kafkaesque.core.storage.RecordHeader;
import eu.kafkaesque.core.storage.StoredRecord;
import eu.kafkaesque.core.storage.TopicStore;
import lombok.RequiredArgsConstructor;
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

import static eu.kafkaesque.core.handler.FetchSessionCoordinator.FINAL_EPOCH;
import static eu.kafkaesque.core.handler.FetchSessionCoordinator.INITIAL_EPOCH;
import static eu.kafkaesque.core.storage.CleanupPolicy.COMPACT;
import static eu.kafkaesque.core.storage.CleanupPolicy.COMPACT_DELETE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.List.of;
import static org.apache.kafka.common.protocol.Errors.FETCH_SESSION_ID_NOT_FOUND;
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
@RequiredArgsConstructor
final class ConsumerDataApiHandler {

    private final EventStore eventStore;
    private final GroupCoordinator groupCoordinator;
    private final TopicStore topicStore;
    private final FetchSessionCoordinator fetchSessionCoordinator;

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
                        .collect(Collectors.toList())))
                .collect(Collectors.toList());

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
                    .collect(Collectors.toList());
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
                        .collect(Collectors.toList())))
                .collect(Collectors.toList());

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
     * <p>Implements KIP-227 incremental fetch sessions. The behaviour depends on the
     * {@code sessionId} and {@code sessionEpoch} fields in the request:</p>
     * <ul>
     *   <li>{@code sessionId=0, sessionEpoch=0} – full fetch; a new session is created and
     *       all requested partitions are returned (even if empty)</li>
     *   <li>{@code sessionId=0, sessionEpoch=-1} – legacy mode; full fetch with no session,
     *       {@code sessionId=0} is returned in the response</li>
     *   <li>{@code sessionId&gt;0, sessionEpoch&gt;0} – incremental fetch; only partitions with
     *       new records since the last fetch are included in the response</li>
     *   <li>{@code sessionId&gt;0, sessionEpoch=-1} – session close; the session is removed and
     *       the response carries {@code sessionId=0}</li>
     * </ul>
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

            final int sessionId = request.sessionId();
            final int epoch = request.sessionEpoch();

            if (sessionId == 0 && epoch == INITIAL_EPOCH) {
                return generateFullFetchResponse(requestHeader, request, true);
            } else if (sessionId == 0) {
                return generateFullFetchResponse(requestHeader, request, false);
            } else if (epoch == FINAL_EPOCH) {
                return generateSessionCloseFetchResponse(requestHeader, request);
            } else {
                return generateIncrementalFetchResponse(requestHeader, request, sessionId, epoch);
            }

        } catch (final Exception e) {
            log.error("Error generating Fetch response", e);
            return null;
        }
    }

    /**
     * Generates a full FETCH response for all partitions in the request.
     *
     * <p>When {@code createSession} is {@code true} a new fetch session is established and
     * its ID is returned in the response; when {@code false} (legacy mode) the response
     * carries {@code sessionId=0}.</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed fetch request
     * @param createSession {@code true} to create a new session, {@code false} for legacy mode
     * @return the serialised response buffer
     */
    private ByteBuffer generateFullFetchResponse(
            final RequestHeader requestHeader,
            final FetchRequestData request,
            final boolean createSession) {
        final var isolationLevel = request.isolationLevel();
        final var topicResponses = request.topics().stream()
            .map(topic -> buildFetchableTopicResponse(topic, isolationLevel))
            .collect(Collectors.toList());

        final int responseSessionId;
        if (createSession) {
            final var partitions = extractPartitions(request.topics());
            final var session = fetchSessionCoordinator.createSession(partitions);
            responseSessionId = session.sessionId();
        } else {
            responseSessionId = 0;
        }

        final var data = new FetchResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode((short) 0)
            .setSessionId(responseSessionId)
            .setResponses(topicResponses);

        return ResponseSerializer.serialize(requestHeader, data, ApiKeys.FETCH);
    }

    /**
     * Generates a FETCH response that closes an existing session.
     *
     * <p>If the session does not exist, {@code FETCH_SESSION_ID_NOT_FOUND} is returned.
     * Otherwise the session is removed and the response carries {@code sessionId=0} with
     * an empty partition list, per KIP-227.</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed fetch request
     * @return the serialised response buffer
     */
    private ByteBuffer generateSessionCloseFetchResponse(
            final RequestHeader requestHeader,
            final FetchRequestData request) {
        if (!fetchSessionCoordinator.closeSession(request.sessionId())) {
            return ResponseSerializer.serialize(requestHeader,
                new FetchResponseData()
                    .setThrottleTimeMs(0)
                    .setErrorCode(FETCH_SESSION_ID_NOT_FOUND.code())
                    .setSessionId(0),
                ApiKeys.FETCH);
        }

        return ResponseSerializer.serialize(requestHeader,
            new FetchResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode((short) 0)
                .setSessionId(0)
                .setResponses(of()),
            ApiKeys.FETCH);
    }

    /**
     * Generates an incremental FETCH response for an existing session.
     *
     * <p>The request's partition list is merged into the session's tracked partitions
     * (forgotten topics are removed). Only partitions that have records available at or
     * beyond their current fetch offset are included in the response; empty partitions are
     * omitted so the client infers no new data from their absence.</p>
     *
     * <p>On session-not-found or invalid-epoch errors the error code is returned in the
     * response and the client is expected to fall back to a full fetch.</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed fetch request
     * @param sessionId     the session ID from the request
     * @param epoch         the session epoch from the request
     * @return the serialised response buffer
     */
    private ByteBuffer generateIncrementalFetchResponse(
            final RequestHeader requestHeader,
            final FetchRequestData request,
            final int sessionId,
            final int epoch) {
        final var updates = extractPartitions(request.topics());
        final var result = fetchSessionCoordinator.updateSession(
            sessionId, epoch, updates, request.forgottenTopicsData());

        if (result.isError()) {
            final var data = new FetchResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode(result.errorCode())
                .setSessionId(0);
            return ResponseSerializer.serialize(requestHeader, data, ApiKeys.FETCH);
        }

        final var isolationLevel = request.isolationLevel();
        final var topicResponses = buildIncrementalTopicResponses(result.session(), isolationLevel);

        final var data = new FetchResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode((short) 0)
            .setSessionId(sessionId)
            .setResponses(topicResponses);

        return ResponseSerializer.serialize(requestHeader, data, ApiKeys.FETCH);
    }

    /**
     * Builds a {@link FetchResponseData.FetchableTopicResponse} for all partitions in the
     * given request topic, including empty partitions.
     *
     * @param topic          the topic from the fetch request
     * @param isolationLevel the isolation level for record filtering
     * @return the populated topic response
     */
    private FetchResponseData.FetchableTopicResponse buildFetchableTopicResponse(
            final FetchRequestData.FetchTopic topic,
            final byte isolationLevel) {
        return new FetchResponseData.FetchableTopicResponse()
            .setTopic(topic.topic())
            .setPartitions(topic.partitions().stream()
                .map(partition -> buildPartitionData(
                    topic.topic(), partition.partition(), partition.fetchOffset(), isolationLevel))
                .collect(Collectors.toList()));
    }

    /**
     * Builds the incremental topic responses for an existing session, including only
     * partitions that have records available at or beyond their tracked fetch offset.
     *
     * @param session        the updated fetch session with the effective partition set
     * @param isolationLevel the isolation level for record filtering
     * @return the list of topic responses containing only partitions with new data
     */
    private List<FetchResponseData.FetchableTopicResponse> buildIncrementalTopicResponses(
            final FetchSession session,
            final byte isolationLevel) {
        final var grouped = session.partitions().entrySet().stream()
            .filter(entry -> hasData(entry.getKey().topic(), entry.getKey().partition(),
                entry.getValue().fetchOffset(), isolationLevel))
            .collect(Collectors.groupingBy(
                entry -> entry.getKey().topic(),
                Collectors.toList()));

        return grouped.entrySet().stream()
            .map(topicEntry -> new FetchResponseData.FetchableTopicResponse()
                .setTopic(topicEntry.getKey())
                .setPartitions(topicEntry.getValue().stream()
                    .map(entry -> buildPartitionData(
                        entry.getKey().topic(), entry.getKey().partition(),
                        entry.getValue().fetchOffset(), isolationLevel))
                    .collect(Collectors.toList())))
            .collect(Collectors.toList());
    }

    /**
     * Builds a {@link FetchResponseData.PartitionData} for a single topic-partition.
     *
     * @param topic          the topic name
     * @param partition      the partition index
     * @param fetchOffset    the offset from which to start fetching
     * @param isolationLevel the isolation level for record filtering
     * @return the populated partition data
     */
    private FetchResponseData.PartitionData buildPartitionData(
            final String topic,
            final int partition,
            final long fetchOffset,
            final byte isolationLevel) {
        final var highWatermark = eventStore.getRecordCount(topic, partition);
        final long lso = (isolationLevel == 1)
            ? eventStore.getLastStableOffset(topic, partition)
            : highWatermark;
        final var records = buildFetchRecords(topic, partition, fetchOffset, isolationLevel, lso);
        return new FetchResponseData.PartitionData()
            .setPartitionIndex(partition)
            .setErrorCode((short) 0)
            .setHighWatermark(highWatermark)
            .setLastStableOffset(lso)
            .setLogStartOffset(0L)
            .setRecords(records);
    }

    /**
     * Returns {@code true} if there are records available for the given topic-partition
     * at or beyond the specified fetch offset, taking the isolation level into account.
     *
     * @param topic          the topic name
     * @param partition      the partition index
     * @param fetchOffset    the offset from which the client wants to start fetching
     * @param isolationLevel {@code 0} for READ_UNCOMMITTED, {@code 1} for READ_COMMITTED
     * @return {@code true} if at least one record is available at or beyond {@code fetchOffset}
     */
    private boolean hasData(
            final String topic, final int partition,
            final long fetchOffset, final byte isolationLevel) {
        final long limit = (isolationLevel == 1)
            ? eventStore.getLastStableOffset(topic, partition)
            : eventStore.getRecordCount(topic, partition);
        return limit > fetchOffset;
    }

    /**
     * Extracts the partition fetch states from the topic list in a fetch request.
     *
     * @param topics the fetch topics from the request
     * @return a map from topic-partition keys to their fetch state
     */
    private Map<TopicPartitionKey, PartitionFetchState> extractPartitions(
            final List<FetchRequestData.FetchTopic> topics) {
        return topics.stream()
            .flatMap(topic -> topic.partitions().stream()
                .map(p -> Map.entry(
                    new TopicPartitionKey(topic.topic(), p.partition()),
                    new PartitionFetchState(p.fetchOffset(), p.partitionMaxBytes()))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
     * <p>If the topics list is {@code null} or empty, all committed offsets for the
     * group are returned (admin {@code listConsumerGroupOffsets} variant).</p>
     *
     * @param groupId the consumer group ID
     * @param topics  the topics to fetch offsets for (v8+ plural request type);
     *                may be null/empty when listing all offsets
     * @return the list of topic responses (v8+ plural response type)
     */
    private List<OffsetFetchResponseData.OffsetFetchResponseTopics> buildOffsetFetchGroupTopicResponses(
            final String groupId,
            final List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics) {

        if (topics == null || topics.isEmpty()) {
            return buildAllCommittedOffsetsResponse(groupId);
        }

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
                    .collect(Collectors.toList())))
            .collect(Collectors.toList());
    }

    /**
     * Builds topic responses containing all committed offsets for a consumer group.
     *
     * @param groupId the consumer group ID
     * @return topic responses for all committed offsets
     */
    private List<OffsetFetchResponseData.OffsetFetchResponseTopics> buildAllCommittedOffsetsResponse(
            final String groupId) {
        return groupCoordinator.getCommittedOffsets(groupId).stream()
            .collect(java.util.stream.Collectors.groupingBy(GroupCoordinator.CommittedOffsetEntry::topic))
            .entrySet().stream()
            .map(entry -> new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName(entry.getKey())
                .setPartitions(entry.getValue().stream()
                    .map(coe -> new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                        .setPartitionIndex(coe.partition())
                        .setCommittedOffset(coe.offset())
                        .setCommittedLeaderEpoch(-1)
                        .setMetadata("")
                        .setErrorCode((short) 0))
                    .collect(Collectors.toList())))
            .collect(Collectors.toList());
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
                    .collect(Collectors.toList())))
            .collect(Collectors.toList());
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
            .collect(Collectors.toList());

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
            .collect(Collectors.toList());
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
            .collect(Collectors.toList());
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
        final var reversed = new java.util.ArrayList<>(records);
        java.util.Collections.reverse(reversed);
        final long[] accumulated = {0L};
        return reversed.stream()
            .takeWhile(r -> {
                accumulated[0] += recordByteSize(r);
                return accumulated[0] <= retentionBytes;
            })
            .sorted(Comparator.comparingLong(StoredRecord::offset))
            .collect(Collectors.toList());
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
                toKafkaHeaders(r.headers())));
        return builder.build();
    }

    /**
     * Converts a list of Kafkaesque {@link RecordHeader} instances to a Kafka {@link Header} array
     * for use in the wire protocol serialisation.
     *
     * @param headers the Kafkaesque headers to convert
     * @return an array of Kafka headers
     */
    private static Header[] toKafkaHeaders(final List<RecordHeader> headers) {
        return headers.stream()
            .map(h -> (Header) new org.apache.kafka.common.header.internals.RecordHeader(h.key(), h.value()))
            .toArray(Header[]::new);
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
