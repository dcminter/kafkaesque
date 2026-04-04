package eu.kafkaesque.core;

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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

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

    /**
     * Creates a new handler backed by the given event store and group coordinator.
     *
     * @param eventStore       the store holding produced records
     * @param groupCoordinator the coordinator managing committed offsets
     */
    ConsumerDataApiHandler(final EventStore eventStore, final GroupCoordinator groupCoordinator) {
        this.eventStore = eventStore;
        this.groupCoordinator = groupCoordinator;
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
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateFetchResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new FetchRequestData(accessor, requestHeader.apiVersion());

            final var topicResponses = request.topics().stream()
                .map(topic -> new FetchResponseData.FetchableTopicResponse()
                    .setTopic(topic.topic())
                    .setPartitions(topic.partitions().stream()
                        .map(partition -> {
                            final var records = buildFetchRecords(
                                topic.topic(), partition.partition(), partition.fetchOffset());
                            final var highWatermark =
                                eventStore.getRecordCount(topic.topic(), partition.partition());
                            return new FetchResponseData.PartitionData()
                                .setPartitionIndex(partition.partition())
                                .setErrorCode((short) 0)
                                .setHighWatermark(highWatermark)
                                .setLastStableOffset(highWatermark)
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
     * Builds a {@link MemoryRecords} instance from stored records at or after the given offset.
     *
     * @param topic       the topic name
     * @param partition   the partition index
     * @param fetchOffset the offset from which to start; records with lower offsets are skipped
     * @return a {@code MemoryRecords} instance, or {@link MemoryRecords#EMPTY} if none qualify
     */
    private MemoryRecords buildFetchRecords(final String topic, final int partition, final long fetchOffset) {
        final var stored = eventStore.getRecords(topic, partition).stream()
            .filter(r -> r.offset() >= fetchOffset)
            .toList();

        if (stored.isEmpty()) {
            return MemoryRecords.EMPTY;
        }

        final long baseOffset = stored.get(0).offset();
        final var buf = ByteBuffer.allocate(stored.size() * 512 + 64);
        final var builder = MemoryRecords.builder(
            buf, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME, baseOffset);

        for (final var record : stored) {
            final byte[] key = record.key() != null ? record.key().getBytes(StandardCharsets.UTF_8) : null;
            final byte[] value = record.value() != null ? record.value().getBytes(StandardCharsets.UTF_8) : null;
            final Header[] headers = record.headers().toArray(Header[]::new);
            builder.append(record.timestamp(), key, value, headers);
        }

        return builder.build();
    }
}
