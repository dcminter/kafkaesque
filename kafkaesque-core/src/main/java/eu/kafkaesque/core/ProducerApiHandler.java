package eu.kafkaesque.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * Handles Kafka producer API responses.
 *
 * <p>Covers {@link ApiKeys#PRODUCE}: parses incoming records, stores them in the
 * event store, and returns a success acknowledgement.</p>
 *
 * @see KafkaProtocolHandler
 * @see EventStore
 */
@Slf4j
final class ProducerApiHandler {

    private final EventStore eventStore;

    /**
     * Creates a new handler backed by the given event store.
     *
     * @param eventStore the store that receives produced records
     */
    ProducerApiHandler(final EventStore eventStore) {
        this.eventStore = eventStore;
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

            logProduceRequest(produceRequest);

            final var topicResponses = new ProduceResponseData.TopicProduceResponseCollection();

            for (final var topicData : produceRequest.topicData()) {
                final var partitionResponses = new ArrayList<ProduceResponseData.PartitionProduceResponse>();

                for (final var partitionData : topicData.partitionData()) {
                    partitionResponses.add(new ProduceResponseData.PartitionProduceResponse()
                        .setIndex(partitionData.index())
                        .setErrorCode((short) 0)
                        .setBaseOffset(0L)
                        .setLogAppendTimeMs(-1L)
                        .setLogStartOffset(0L));
                }

                topicResponses.add(new ProduceResponseData.TopicProduceResponse()
                    .setName(topicData.name())
                    .setPartitionResponses(partitionResponses));
            }

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
     * Parses a PRODUCE request, logs its contents, and stores each record in the event store.
     *
     * @param produceRequest the parsed produce request
     */
    private void logProduceRequest(final ProduceRequestData produceRequest) {
        log.info("Received PRODUCE request: transactionalId={}, acks={}, timeoutMs={}",
            produceRequest.transactionalId(), produceRequest.acks(), produceRequest.timeoutMs());

        for (final var topicData : produceRequest.topicData()) {
            log.info("  Topic: {}", topicData.name());

            for (final var partitionData : topicData.partitionData()) {
                log.info("    Partition: {}", partitionData.index());

                if (partitionData.records() instanceof MemoryRecords memoryRecords) {
                    var count = 0;
                    for (final var batch : memoryRecords.batches()) {
                        for (final var record : batch) {
                            count++;
                            final var key = readBufferToString(record.key(), record.keySize());
                            final var value = readBufferToString(record.value(), record.valueSize());
                            final var offset = eventStore.storeRecord(
                                topicData.name(), partitionData.index(), record.timestamp(), key, value);
                            log.info("      Record {}: offset={}, key='{}', value='{}'", count, offset, key, value);
                        }
                    }
                    log.info("    Total records: {}", count);
                }
            }
        }
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
        return new String(raw, StandardCharsets.UTF_8);
    }
}
