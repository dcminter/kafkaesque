package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.EventStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
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
            produceRequest.topicData().stream()
                .map(topicData -> new ProduceResponseData.TopicProduceResponse()
                    .setName(topicData.name())
                    .setPartitionResponses(topicData.partitionData().stream()
                        .map(partitionData -> new ProduceResponseData.PartitionProduceResponse()
                            .setIndex(partitionData.index())
                            .setErrorCode((short) 0)
                            .setBaseOffset(0L)
                            .setLogAppendTimeMs(-1L)
                            .setLogStartOffset(0L))
                        .toList()))
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
                            final var headers = readHeaders(record.headers());
                            final var offset = eventStore.storeRecord(
                                topicData.name(), partitionData.index(), record.timestamp(), key, value, headers);
                            log.info("      Record {}: offset={}, key='{}', value='{}', headers={}",
                                count, offset, key, value, headers.size());
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
