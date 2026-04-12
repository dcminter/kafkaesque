package eu.kafkaesque.core.handler;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Tracks per-producer, per-partition idempotency state to prevent duplicate record storage
 * when the Kafka producer retries after a lost acknowledgement.
 *
 * <p>Kafka's idempotent producer assigns each record batch a {@code baseSequence} that
 * increments monotonically within a {@code (producerId, producerEpoch)} lifetime. When the
 * broker receives a batch whose {@code baseSequence} matches the last acknowledged sequence
 * for that producer and partition, the batch is a duplicate and the cached response is
 * returned without a second write.</p>
 *
 * <p>Idempotency state is tracked per {@code (producerId, producerEpoch, topic, partition)}
 * tuple. Non-idempotent batches (where {@code producerId == RecordBatch.NO_PRODUCER_ID})
 * bypass tracking entirely.</p>
 *
 * <p>Thread-safe: all state mutations use {@link ConcurrentHashMap#compute} to guarantee
 * atomicity of the check-and-update cycle.</p>
 */
@Slf4j
final class IdempotentProducerRegistry {

    /** Modulus for wrapping sequence numbers: {@code 2^31}. */
    private static final long SEQUENCE_MODULUS = (long) Integer.MAX_VALUE + 1;

    /**
     * Result of processing one incoming batch through the idempotency registry.
     *
     * <p>Callers switch on the three subtypes to decide whether to store the batch
     * ({@link Store}), return a cached response ({@link Duplicate}), or surface an
     * error ({@link Error}).</p>
     */
    interface CheckResult {

        /**
         * The batch was new and the store action was executed.
         */
        @EqualsAndHashCode
        @ToString
        static final class Store implements CheckResult {

            /** The first offset assigned by the event store. */
            private final long baseOffset;

            /**
             * Creates a new {@code Store} result.
             *
             * @param baseOffset the first offset assigned by the event store
             */
            Store(final long baseOffset) {
                this.baseOffset = baseOffset;
            }

            /**
             * Returns the first offset assigned by the event store.
             *
             * @return the first offset assigned by the event store
             */
            long baseOffset() {
                return baseOffset;
            }
        }

        /**
         * The batch is a duplicate of one already stored.
         */
        @EqualsAndHashCode
        @ToString
        static final class Duplicate implements CheckResult {

            /** The first offset from the original store. */
            private final long cachedBaseOffset;

            /**
             * Creates a new {@code Duplicate} result.
             *
             * @param cachedBaseOffset the first offset from the original store
             */
            Duplicate(final long cachedBaseOffset) {
                this.cachedBaseOffset = cachedBaseOffset;
            }

            /**
             * Returns the first offset from the original store.
             *
             * @return the first offset from the original store
             */
            long cachedBaseOffset() {
                return cachedBaseOffset;
            }
        }

        /**
         * The batch is invalid.
         */
        @EqualsAndHashCode
        @ToString
        static final class Error implements CheckResult {

            /** The Kafka protocol error to return to the client. */
            private final Errors errorCode;

            /**
             * Creates a new {@code Error} result.
             *
             * @param errorCode the Kafka protocol error to return to the client
             */
            Error(final Errors errorCode) {
                this.errorCode = errorCode;
            }

            /**
             * Returns the Kafka protocol error to return to the client.
             *
             * @return the Kafka protocol error to return to the client
             */
            Errors errorCode() {
                return errorCode;
            }
        }
    }

    /**
     * Key identifying the idempotency state for a single producer epoch on one partition.
     */
    @EqualsAndHashCode
    @ToString
    private static final class ProducerPartitionKey {

        /** The long producer ID assigned by {@code INIT_PRODUCER_ID}. */
        private final long producerId;

        /** The producer epoch. */
        private final short producerEpoch;

        /** The topic name. */
        private final String topic;

        /** The partition index. */
        private final int partition;

        /**
         * Creates a new {@code ProducerPartitionKey}.
         *
         * @param producerId    the long producer ID assigned by {@code INIT_PRODUCER_ID}
         * @param producerEpoch the producer epoch
         * @param topic         the topic name
         * @param partition     the partition index
         */
        ProducerPartitionKey(
                final long producerId, final short producerEpoch,
                final String topic, final int partition) {
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.topic = topic;
            this.partition = partition;
        }

        /**
         * Returns the producer ID.
         *
         * @return the producer ID
         */
        long producerId() {
            return producerId;
        }

        /**
         * Returns the producer epoch.
         *
         * @return the producer epoch
         */
        short producerEpoch() {
            return producerEpoch;
        }

        /**
         * Returns the topic name.
         *
         * @return the topic name
         */
        String topic() {
            return topic;
        }

        /**
         * Returns the partition index.
         *
         * @return the partition index
         */
        int partition() {
            return partition;
        }
    }

    /**
     * State for one {@link ProducerPartitionKey}: the last committed batch's sequence
     * number, record count, and first assigned offset.
     */
    @EqualsAndHashCode
    @ToString
    private static final class ProducerPartitionState {

        /** The {@code baseSequence} of the last stored batch. */
        private final int lastBaseSequence;

        /** The number of records in the last stored batch. */
        private final int lastBatchCount;

        /** The first offset assigned to the last stored batch. */
        private final long cachedBaseOffset;

        /**
         * Creates a new {@code ProducerPartitionState}.
         *
         * @param lastBaseSequence the {@code baseSequence} of the last stored batch
         * @param lastBatchCount   the number of records in the last stored batch
         * @param cachedBaseOffset the first offset assigned to the last stored batch
         */
        ProducerPartitionState(final int lastBaseSequence, final int lastBatchCount, final long cachedBaseOffset) {
            this.lastBaseSequence = lastBaseSequence;
            this.lastBatchCount = lastBatchCount;
            this.cachedBaseOffset = cachedBaseOffset;
        }

        /**
         * Returns the {@code baseSequence} of the last stored batch.
         *
         * @return the last base sequence
         */
        int lastBaseSequence() {
            return lastBaseSequence;
        }

        /**
         * Returns the number of records in the last stored batch.
         *
         * @return the last batch count
         */
        int lastBatchCount() {
            return lastBatchCount;
        }

        /**
         * Returns the first offset assigned to the last stored batch.
         *
         * @return the cached base offset
         */
        long cachedBaseOffset() {
            return cachedBaseOffset;
        }
    }

    /** Per-partition idempotency state, keyed by (producerId, epoch, topic, partition). */
    private final Map<ProducerPartitionKey, ProducerPartitionState> state = new ConcurrentHashMap<>();

    /** Highest epoch seen per producerId, used to fence requests with stale epochs. */
    private final Map<Long, Short> highestSeenEpoch = new ConcurrentHashMap<>();

    /**
     * Processes an incoming batch against recorded idempotency state.
     *
     * <p>If {@code producerId == RecordBatch.NO_PRODUCER_ID} the batch is non-idempotent:
     * {@code storeAction} is called immediately and {@link CheckResult.Store} is returned.
     * Otherwise the incoming {@code baseSequence} is validated against the last committed
     * state for the {@code (producerId, producerEpoch, topic, partition)} tuple:</p>
     * <ul>
     *   <li>If the epoch is lower than the highest seen for this producer, returns
     *       {@link CheckResult.Error} with {@link Errors#INVALID_PRODUCER_EPOCH}.</li>
     *   <li>If the batch is the first for this key and {@code baseSequence != 0}, returns
     *       {@link CheckResult.Error} with {@link Errors#OUT_OF_ORDER_SEQUENCE_NUMBER}.</li>
     *   <li>If {@code baseSequence} matches the last committed sequence, returns
     *       {@link CheckResult.Duplicate} with the cached offset without calling
     *       {@code storeAction}.</li>
     *   <li>If {@code baseSequence} is not the expected next value, returns
     *       {@link CheckResult.Error} with {@link Errors#OUT_OF_ORDER_SEQUENCE_NUMBER}.</li>
     *   <li>Otherwise calls {@code storeAction} exactly once and returns
     *       {@link CheckResult.Store} with the resulting offset.</li>
     * </ul>
     *
     * @param producerId    the producer ID from the record batch header
     * @param producerEpoch the producer epoch from the record batch header
     * @param topic         the topic name
     * @param partition     the partition index
     * @param baseSequence  the {@code baseSequence} from the record batch header
     * @param batchCount    the number of records in the batch
     * @param storeAction   called exactly once if the batch is new; returns the first offset
     * @return a {@link CheckResult} indicating the outcome
     */
    CheckResult process(
            final long producerId,
            final short producerEpoch,
            final String topic,
            final int partition,
            final int baseSequence,
            final int batchCount,
            final LongSupplier storeAction) {

        if (producerId == RecordBatch.NO_PRODUCER_ID) {
            return new CheckResult.Store(storeAction.getAsLong());
        }

        log.info("Idempotency check: producerId={}, epoch={}, topic={}, partition={}, baseSeq={}, count={}",
            producerId, producerEpoch, topic, partition, baseSequence, batchCount);

        final var epochResult = validateEpoch(producerId, producerEpoch);
        if (epochResult != null) {
            return epochResult;
        }

        return computePartitionResult(
            new ProducerPartitionKey(producerId, producerEpoch, topic, partition),
            baseSequence, batchCount, storeAction);
    }

    /**
     * Validates the producer epoch against the highest seen for this producer ID.
     *
     * @param producerId    the producer ID
     * @param producerEpoch the epoch to validate
     * @return {@link CheckResult.Error} if the epoch is stale; {@code null} if valid
     */
    private CheckResult validateEpoch(final long producerId, final short producerEpoch) {
        final CheckResult[] result = {null};
        highestSeenEpoch.compute(producerId, (id, highest) -> {
            if (highest != null && producerEpoch < highest) {
                result[0] = new CheckResult.Error(Errors.INVALID_PRODUCER_EPOCH);
                return highest;
            }
            return producerEpoch;
        });
        return result[0];
    }

    /**
     * Atomically checks and updates the per-partition state, calling {@code storeAction}
     * only for genuinely new batches.
     *
     * @param key          the producer/epoch/partition key
     * @param baseSequence the incoming base sequence number
     * @param batchCount   the number of records in the batch
     * @param storeAction  the action to execute if the batch is new
     * @return the check result
     */
    private CheckResult computePartitionResult(
            final ProducerPartitionKey key,
            final int baseSequence,
            final int batchCount,
            final LongSupplier storeAction) {

        final CheckResult[] result = {null};
        state.compute(key, (k, existing) -> {
            if (existing == null) {
                return handleNewProducer(baseSequence, batchCount, storeAction, result);
            }
            return handleExistingState(existing, baseSequence, batchCount, storeAction, result);
        });
        return result[0];
    }

    /**
     * Handles the first batch seen for a {@code (producerId, epoch, topic, partition)}.
     * The first batch must have {@code baseSequence == 0}.
     *
     * @param baseSequence the incoming base sequence number
     * @param batchCount   the number of records in the batch
     * @param storeAction  the store supplier
     * @param result       single-element array used to return the {@link CheckResult}
     * @return the new state entry, or {@code null} if the batch was rejected
     */
    private static ProducerPartitionState handleNewProducer(
            final int baseSequence,
            final int batchCount,
            final LongSupplier storeAction,
            final CheckResult[] result) {
        if (baseSequence != 0) {
            log.warn("First batch for producer has baseSequence={} (expected 0)", baseSequence);
            result[0] = new CheckResult.Error(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER);
            return null;
        }
        final long offset = storeAction.getAsLong();
        result[0] = new CheckResult.Store(offset);
        return new ProducerPartitionState(baseSequence, batchCount, offset);
    }

    /**
     * Handles a subsequent batch for a producer/partition with existing committed state.
     *
     * @param existing     the last committed state for this key
     * @param baseSequence the incoming base sequence number
     * @param batchCount   the number of records in the batch
     * @param storeAction  the store supplier
     * @param result       single-element array used to return the {@link CheckResult}
     * @return the updated state entry, or the existing entry if unchanged
     */
    private static ProducerPartitionState handleExistingState(
            final ProducerPartitionState existing,
            final int baseSequence,
            final int batchCount,
            final LongSupplier storeAction,
            final CheckResult[] result) {
        if (baseSequence == existing.lastBaseSequence()) {
            log.debug("Duplicate batch: baseSequence={}, returning cachedOffset={}",
                baseSequence, existing.cachedBaseOffset());
            result[0] = new CheckResult.Duplicate(existing.cachedBaseOffset());
            return existing;
        }
        final int expectedNext = nextSequence(existing.lastBaseSequence(), existing.lastBatchCount());
        if (baseSequence != expectedNext) {
            log.warn("Out-of-order sequence: expected={}, got={}", expectedNext, baseSequence);
            result[0] = new CheckResult.Error(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER);
            return existing;
        }
        final long offset = storeAction.getAsLong();
        result[0] = new CheckResult.Store(offset);
        return new ProducerPartitionState(baseSequence, batchCount, offset);
    }

    /**
     * Computes the expected next {@code baseSequence} after a batch of {@code batchCount}
     * records starting at {@code baseSequence}, wrapping at {@link Integer#MAX_VALUE}.
     *
     * @param baseSequence the current base sequence
     * @param batchCount   the number of records in the batch
     * @return the expected next base sequence
     */
    private static int nextSequence(final int baseSequence, final int batchCount) {
        return (int) ((baseSequence + (long) batchCount) % SEQUENCE_MODULUS);
    }
}
