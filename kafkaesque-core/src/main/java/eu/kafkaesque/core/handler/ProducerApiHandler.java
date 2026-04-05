package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.EventStore;
import eu.kafkaesque.core.storage.RecordData;
import eu.kafkaesque.core.storage.TopicStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Handles Kafka producer API responses.
 *
 * <p>Covers {@link ApiKeys#PRODUCE}: parses incoming records, stores them in the
 * event store, and returns a success acknowledgement.</p>
 *
 * <p>Non-transactional records are stored immediately and are visible to all consumers.
 * Records produced with a {@code transactionalId} are stored as
 * {@link eu.kafkaesque.core.storage.TransactionState#PENDING} and only become visible
 * once the owning transaction is committed via {@code END_TXN}.</p>
 *
 * @see KafkaProtocolHandler
 * @see EventStore
 */
@Slf4j
final class ProducerApiHandler {

    private final EventStore eventStore;
    private final TopicStore topicStore;
    private final boolean autoCreateTopicsEnabled;

    /**
     * Creates a new handler backed by the given event store, with auto-topic-creation enabled.
     *
     * @param eventStore the store that receives produced records
     */
    ProducerApiHandler(final EventStore eventStore) {
        this(eventStore, null, true);
    }

    /**
     * Creates a new handler backed by the given event store and topic store.
     *
     * @param eventStore              the store that receives produced records
     * @param topicStore              the topic registry, consulted when auto-create is disabled;
     *                                may be {@code null} when {@code autoCreateTopicsEnabled} is {@code true}
     * @param autoCreateTopicsEnabled {@code false} to reject produces to topics not in {@code topicStore}
     */
    ProducerApiHandler(
            final EventStore eventStore,
            final TopicStore topicStore,
            final boolean autoCreateTopicsEnabled) {
        this.eventStore = eventStore;
        this.topicStore = topicStore;
        this.autoCreateTopicsEnabled = autoCreateTopicsEnabled;
    }

    /**
     * Generates a PRODUCE response after storing all published records.
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or null on error
     */
    ByteBuffer generateProduceResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var produceRequest = new ProduceRequestData(accessor, requestHeader.apiVersion());

            final var transactionalId = produceRequest.transactionalId();
            final var isTransactional = transactionalId != null && !transactionalId.isBlank();

            log.info("Received PRODUCE request: transactionalId={}, acks={}, timeoutMs={}",
                transactionalId, produceRequest.acks(), produceRequest.timeoutMs());

            final var topicResponses = new ProduceResponseData.TopicProduceResponseCollection();
            produceRequest.topicData().stream()
                .map(topicData -> buildTopicResponse(topicData, transactionalId, isTransactional))
                .forEach(topicResponses::add);

            final var response = new ProduceResponseData()
                .setThrottleTimeMs(0)
                .setResponses(topicResponses);

            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.PRODUCE);

        } catch (final Exception e) {
            log.error("Error generating Produce response", e);
            return null;
        }
    }

    /**
     * Builds the response for a single topic, storing all its records.
     *
     * @param topicData       the topic data from the produce request
     * @param transactionalId the transactional ID, or null/blank for non-transactional
     * @param isTransactional whether this is a transactional produce
     * @return the topic produce response
     */
    private ProduceResponseData.TopicProduceResponse buildTopicResponse(
            final ProduceRequestData.TopicProduceData topicData,
            final String transactionalId,
            final boolean isTransactional) {

        log.info("  Topic: {}", topicData.name());

        return new ProduceResponseData.TopicProduceResponse()
            .setName(topicData.name())
            .setPartitionResponses(topicData.partitionData().stream()
                .map(partitionData -> buildPartitionResponse(
                    topicData.name(), partitionData, transactionalId, isTransactional))
                .toList());
    }

    /**
     * Builds the response for a single partition, storing its records.
     *
     * @param topicName       the topic name
     * @param partitionData   the partition data from the produce request
     * @param transactionalId the transactional ID, or null/blank for non-transactional
     * @param isTransactional whether this is a transactional produce
     * @return the partition produce response
     */
    private ProduceResponseData.PartitionProduceResponse buildPartitionResponse(
            final String topicName,
            final ProduceRequestData.PartitionProduceData partitionData,
            final String transactionalId,
            final boolean isTransactional) {

        log.info("    Partition: {}", partitionData.index());

        if (!autoCreateTopicsEnabled && (topicStore == null || !topicStore.hasTopic(topicName))) {
            log.warn("    Rejecting produce to unknown topic '{}': auto.create.topics.enable=false", topicName);
            return new ProduceResponseData.PartitionProduceResponse()
                .setIndex(partitionData.index())
                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                .setBaseOffset(-1L)
                .setLogAppendTimeMs(-1L)
                .setLogStartOffset(-1L);
        }

        long baseOffset = -1L;

        if (partitionData.records() instanceof MemoryRecords memoryRecords) {
            var count = 0;
            for (final var batch : memoryRecords.batches()) {
                for (final var record : batch) {
                    count++;
                    final var key = readBufferToString(record.key(), record.keySize());
                    final var value = readBufferToString(record.value(), record.valueSize());
                    final var recordData = new RecordData(
                        topicName, partitionData.index(), record.timestamp(),
                        key, value, readHeaders(record.headers()));

                    final long offset;
                    if (isTransactional) {
                        offset = eventStore.storePendingRecord(transactionalId, recordData);
                    } else {
                        offset = eventStore.storeRecord(recordData);
                    }

                    if (baseOffset < 0) {
                        baseOffset = offset;
                    }
                    log.info("      Record {}: offset={}, key='{}', value='{}', headers={}, transactional={}",
                        count, offset, key, value, recordData.headers().size(), isTransactional);
                }
            }
            log.info("    Total records: {}", count);
        }

        return new ProduceResponseData.PartitionProduceResponse()
            .setIndex(partitionData.index())
            .setErrorCode((short) 0)
            .setBaseOffset(Math.max(baseOffset, 0L))
            .setLogAppendTimeMs(-1L)
            .setLogStartOffset(0L);
    }

    /**
     * Reads bytes from a {@link ByteBuffer} into a String.
     *
     * @param buf  the source buffer (may be null)
     * @param size the number of bytes to read
     * @return the decoded string, or null if {@code buf} is null
     */
    private static String readBufferToString(final ByteBuffer buf, final int size) {
        if (buf == null) {
            return null;
        }
        final byte[] raw = new byte[size];
        buf.get(raw);
        return new String(raw, UTF_8);
    }

    /**
     * Copies the headers from a record's header array into an immutable list.
     *
     * @param headerArray the header array from a decoded record (may be null)
     * @return an immutable list of headers; empty if {@code headerArray} is null or empty
     */
    private static List<Header> readHeaders(final Header[] headerArray) {
        if (headerArray == null || headerArray.length == 0) {
            return List.of();
        }
        return List.of(headerArray);
    }
}
