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
import org.apache.kafka.common.record.RecordBatch;
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
 * <p>Idempotent producers (those with a valid {@code producerId}) are deduplicated at the
 * batch level: if a retry arrives with the same {@code (producerId, producerEpoch,
 * baseSequence)} as the last committed batch for a partition, the cached offset is returned
 * without storing the records again.</p>
 *
 * @see KafkaProtocolHandler
 * @see EventStore
 * @see IdempotentProducerRegistry
 */
@Slf4j
final class ProducerApiHandler {

    private final EventStore eventStore;
    private final TopicStore topicStore;
    private final boolean autoCreateTopicsEnabled;
    private final IdempotentProducerRegistry idempotentProducerRegistry;

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
        this(eventStore, topicStore, autoCreateTopicsEnabled, new IdempotentProducerRegistry());
    }

    /**
     * Creates a new handler with explicit dependencies, including the idempotency registry.
     *
     * @param eventStore                  the store that receives produced records
     * @param topicStore                  the topic registry; may be {@code null} when auto-create is enabled
     * @param autoCreateTopicsEnabled     {@code false} to reject produces to unknown topics
     * @param idempotentProducerRegistry  the registry used to detect and suppress duplicate batches
     */
    ProducerApiHandler(
            final EventStore eventStore,
            final TopicStore topicStore,
            final boolean autoCreateTopicsEnabled,
            final IdempotentProducerRegistry idempotentProducerRegistry) {
        this.eventStore = eventStore;
        this.topicStore = topicStore;
        this.autoCreateTopicsEnabled = autoCreateTopicsEnabled;
        this.idempotentProducerRegistry = idempotentProducerRegistry;
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
     * <p>Each record batch is processed through the {@link IdempotentProducerRegistry}.
     * Duplicate batches are acknowledged with the original offset; out-of-order or
     * epoch-fenced batches return an error without storing any records.</p>
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
            return errorPartitionResponse(partitionData.index(), Errors.UNKNOWN_TOPIC_OR_PARTITION);
        }

        if (!(partitionData.records() instanceof MemoryRecords memoryRecords)) {
            return successPartitionResponse(partitionData.index(), 0L);
        }

        long baseOffset = -1L;
        for (final var batch : memoryRecords.batches()) {
            log.info("    Batch: producerId={}, epoch={}, baseSeq={}, hasProducerId={}",
                batch.producerId(), batch.producerEpoch(), batch.baseSequence(), batch.hasProducerId());
            final var batchResult = processBatch(
                batch, topicName, partitionData.index(), transactionalId, isTransactional);
            if (batchResult instanceof IdempotentProducerRegistry.CheckResult.Error error) {
                log.warn("    Idempotency check failed for partition {}: {}",
                    partitionData.index(), error.errorCode());
                return errorPartitionResponse(partitionData.index(), error.errorCode());
            } else if (batchResult instanceof IdempotentProducerRegistry.CheckResult.Duplicate dup) {
                log.debug("    Duplicate batch suppressed for partition {}, offset={}",
                    partitionData.index(), dup.cachedBaseOffset());
                if (baseOffset < 0) {
                    baseOffset = dup.cachedBaseOffset();
                }
            } else if (batchResult instanceof IdempotentProducerRegistry.CheckResult.Store store) {
                if (baseOffset < 0) {
                    baseOffset = store.baseOffset();
                }
            }
        }

        return successPartitionResponse(partitionData.index(), Math.max(baseOffset, 0L));
    }

    /**
     * Delegates a single record batch to the {@link IdempotentProducerRegistry}, which
     * either stores the batch, returns a cached duplicate result, or returns an error.
     *
     * @param batch           the record batch to process
     * @param topicName       the topic name
     * @param partitionIndex  the partition index
     * @param transactionalId the transactional ID, or null for non-transactional produces
     * @param isTransactional whether this is a transactional produce
     * @return the idempotency check result
     */
    private IdempotentProducerRegistry.CheckResult processBatch(
            final RecordBatch batch,
            final String topicName,
            final int partitionIndex,
            final String transactionalId,
            final boolean isTransactional) {
        return idempotentProducerRegistry.process(
            batch.producerId(), batch.producerEpoch(),
            topicName, partitionIndex,
            batch.baseSequence(), determineBatchCount(batch),
            () -> storeBatch(batch, topicName, partitionIndex, transactionalId, isTransactional));
    }

    /**
     * Stores every record in {@code batch} and returns the first offset assigned.
     *
     * @param batch           the batch to store
     * @param topicName       the topic name
     * @param partitionIndex  the partition index
     * @param transactionalId the transactional ID, or null for non-transactional produces
     * @param isTransactional whether to use pending (transactional) storage
     * @return the first offset assigned, or {@code -1} if the batch contained no records
     */
    private long storeBatch(
            final RecordBatch batch,
            final String topicName,
            final int partitionIndex,
            final String transactionalId,
            final boolean isTransactional) {
        long firstOffset = -1L;
        var count = 0;
        for (final var record : batch) {
            count++;
            final var key = readBufferToString(record.key(), record.keySize());
            final var value = readBufferToString(record.value(), record.valueSize());
            final var recordData = new RecordData(
                topicName, partitionIndex, record.timestamp(),
                key, value, readHeaders(record.headers()));
            final long offset = isTransactional
                ? eventStore.storePendingRecord(transactionalId, recordData)
                : eventStore.storeRecord(recordData);
            if (firstOffset < 0) {
                firstOffset = offset;
            }
            log.info("      Record {}: offset={}, key='{}', value='{}', headers={}, transactional={}",
                count, offset, key, value, recordData.headers().size(), isTransactional);
        }
        log.info("    Total records in batch: {}", count);
        return firstOffset;
    }

    /**
     * Returns the number of records in {@code batch}, using the batch header count when
     * available (magic v2 batches) or iterating as a fallback.
     *
     * @param batch the record batch
     * @return the number of records
     */
    private static int determineBatchCount(final RecordBatch batch) {
        final var count = batch.countOrNull();
        if (count != null) {
            return count;
        }
        var n = 0;
        for (final var ignored : batch) {
            n++;
        }
        return n;
    }

    /**
     * Constructs an error partition response with the given error code and no valid offsets.
     *
     * @param partitionIndex the partition index
     * @param errorCode      the error to report
     * @return the error response
     */
    private static ProduceResponseData.PartitionProduceResponse errorPartitionResponse(
            final int partitionIndex, final Errors errorCode) {
        return new ProduceResponseData.PartitionProduceResponse()
            .setIndex(partitionIndex)
            .setErrorCode(errorCode.code())
            .setBaseOffset(-1L)
            .setLogAppendTimeMs(-1L)
            .setLogStartOffset(-1L);
    }

    /**
     * Constructs a successful partition response with the given base offset.
     *
     * @param partitionIndex the partition index
     * @param baseOffset     the first offset assigned to this batch
     * @return the success response
     */
    private static ProduceResponseData.PartitionProduceResponse successPartitionResponse(
            final int partitionIndex, final long baseOffset) {
        return new ProduceResponseData.PartitionProduceResponse()
            .setIndex(partitionIndex)
            .setErrorCode((short) 0)
            .setBaseOffset(baseOffset)
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
