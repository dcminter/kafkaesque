package eu.kafkaesque.core.handler;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link IdempotentProducerRegistry}.
 *
 * <p>All tests use a simple incrementing counter as the {@code storeAction} so that each
 * genuine store call receives a distinct, predictable offset.</p>
 */
class IdempotentProducerRegistryTest {

    private static final long PRODUCER_ID = 42L;
    private static final short EPOCH_ZERO = 0;
    private static final short EPOCH_ONE = 1;
    private static final String TOPIC = "test-topic";
    private static final int PARTITION_ZERO = 0;
    private static final int PARTITION_ONE = 1;

    private IdempotentProducerRegistry registry;
    private AtomicLong offsetCounter;

    /** Creates a fresh registry and offset counter for each test. */
    @BeforeEach
    void setUp() {
        registry = new IdempotentProducerRegistry();
        offsetCounter = new AtomicLong(0L);
    }

    /**
     * Verifies that a batch with {@code NO_PRODUCER_ID} bypasses idempotency tracking
     * and is stored immediately.
     */
    @Test
    void process_nonIdempotentBatch_shouldBypassTrackingAndStore() {
        final var result = registry.process(
            RecordBatch.NO_PRODUCER_ID, (short) 0, TOPIC, PARTITION_ZERO, 0, 1,
            offsetCounter::getAndIncrement);

        assertThat(result)
            .isInstanceOf(IdempotentProducerRegistry.CheckResult.Store.class);
        assertThat(((IdempotentProducerRegistry.CheckResult.Store) result).baseOffset())
            .isZero();
    }

    /**
     * Verifies that the first batch for a producer at {@code baseSequence=0} is stored
     * successfully.
     */
    @Test
    void process_firstBatch_withZeroSequence_shouldStore() {
        final var result = registry.process(
            PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 0, 1, offsetCounter::getAndIncrement);

        assertThat(result)
            .isInstanceOf(IdempotentProducerRegistry.CheckResult.Store.class);
        assertThat(((IdempotentProducerRegistry.CheckResult.Store) result).baseOffset())
            .isZero();
    }

    /**
     * Verifies that the very first batch for a producer with {@code baseSequence != 0}
     * returns {@link Errors#OUT_OF_ORDER_SEQUENCE_NUMBER}.
     */
    @Test
    void process_firstBatch_withNonZeroSequence_shouldReturnOutOfOrderError() {
        final var result = registry.process(
            PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 5, 1, offsetCounter::getAndIncrement);

        assertThat(result)
            .isEqualTo(new IdempotentProducerRegistry.CheckResult.Error(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER));
    }

    /**
     * Verifies that three consecutive batches with correct, incrementing sequence numbers
     * are all stored with their expected offsets.
     */
    @Test
    void process_sequentialBatches_shouldStoreAll() {
        final var r0 = registry.process(
            PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 0, 1, offsetCounter::getAndIncrement);
        final var r1 = registry.process(
            PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 1, 1, offsetCounter::getAndIncrement);
        final var r2 = registry.process(
            PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 2, 1, offsetCounter::getAndIncrement);

        assertThat(r0).isInstanceOf(IdempotentProducerRegistry.CheckResult.Store.class);
        assertThat(r1).isInstanceOf(IdempotentProducerRegistry.CheckResult.Store.class);
        assertThat(r2).isInstanceOf(IdempotentProducerRegistry.CheckResult.Store.class);
        assertThat(((IdempotentProducerRegistry.CheckResult.Store) r0).baseOffset()).isZero();
        assertThat(((IdempotentProducerRegistry.CheckResult.Store) r1).baseOffset()).isOne();
        assertThat(((IdempotentProducerRegistry.CheckResult.Store) r2).baseOffset()).isEqualTo(2L);
    }

    /**
     * Verifies that a duplicate batch (same {@code baseSequence} as the last committed batch)
     * returns a {@link IdempotentProducerRegistry.CheckResult.Duplicate} with the original
     * cached offset, without invoking the store action a second time.
     */
    @Test
    void process_duplicateBatch_shouldReturnDuplicateWithCachedOffset() {
        registry.process(PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 0, 1,
            offsetCounter::getAndIncrement);

        // A second call with the same baseSequence must return the cached offset, not a new one
        final var duplicate = registry.process(
            PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 0, 1, () -> 999L);

        assertThat(duplicate)
            .isInstanceOf(IdempotentProducerRegistry.CheckResult.Duplicate.class);
        assertThat(((IdempotentProducerRegistry.CheckResult.Duplicate) duplicate).cachedBaseOffset())
            .as("duplicate must return the original offset, not the value from the store action")
            .isZero();
    }

    /**
     * Verifies that a gap in sequence numbers (skipping an expected value) returns
     * {@link Errors#OUT_OF_ORDER_SEQUENCE_NUMBER}.
     */
    @Test
    void process_gapInSequence_shouldReturnOutOfOrderError() {
        registry.process(PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 0, 1,
            offsetCounter::getAndIncrement);

        // seq=1 expected, seq=2 sent
        final var result = registry.process(
            PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 2, 1, offsetCounter::getAndIncrement);

        assertThat(result)
            .isEqualTo(new IdempotentProducerRegistry.CheckResult.Error(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER));
    }

    /**
     * Verifies that a produce request carrying a stale epoch (lower than the highest seen for
     * that producer) returns {@link Errors#INVALID_PRODUCER_EPOCH}.
     */
    @Test
    void process_staleEpoch_shouldReturnInvalidProducerEpochError() {
        // Establish epoch 0, then bump to epoch 1
        registry.process(PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 0, 1,
            offsetCounter::getAndIncrement);
        registry.process(PRODUCER_ID, EPOCH_ONE,  TOPIC, PARTITION_ZERO, 0, 1,
            offsetCounter::getAndIncrement);

        // A subsequent request using the stale epoch 0 must be fenced
        final var result = registry.process(
            PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 1, 1, offsetCounter::getAndIncrement);

        assertThat(result)
            .isEqualTo(new IdempotentProducerRegistry.CheckResult.Error(Errors.INVALID_PRODUCER_EPOCH));
    }

    /**
     * Verifies that after an epoch is bumped the first batch for the new epoch must have
     * {@code baseSequence=0}, even if the previous epoch had advanced to a higher sequence.
     *
     * <p>This is guaranteed by the {@link IdempotentProducerRegistry} because the partition
     * key includes the epoch; new epoch state is initialised fresh with no prior history.</p>
     */
    @Test
    void process_newHigherEpoch_firstBatchMustStartAtZero() {
        // epoch 0: produce two batches advancing the sequence
        registry.process(PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 0, 1,
            offsetCounter::getAndIncrement);
        registry.process(PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 1, 1,
            offsetCounter::getAndIncrement);

        // epoch 1: first batch at seq=0 must succeed (fresh state for new epoch)
        final var result = registry.process(
            PRODUCER_ID, EPOCH_ONE, TOPIC, PARTITION_ZERO, 0, 1, offsetCounter::getAndIncrement);

        assertThat(result).isInstanceOf(IdempotentProducerRegistry.CheckResult.Store.class);
    }

    /**
     * Verifies that two different partitions each maintain their own independent sequence
     * tracking: seq=0 is valid on partition 1 even after partition 0 has already advanced,
     * and each partition can continue independently.
     */
    @Test
    void process_twoPartitions_shouldTrackSequencesIndependently() {
        final var p0b0 = registry.process(
            PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 0, 1, offsetCounter::getAndIncrement);
        final var p1b0 = registry.process(
            PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ONE,  0, 1, offsetCounter::getAndIncrement);
        final var p0b1 = registry.process(
            PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO, 1, 1, offsetCounter::getAndIncrement);
        final var p1b1 = registry.process(
            PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ONE,  1, 1, offsetCounter::getAndIncrement);

        assertThat(p0b0).isInstanceOf(IdempotentProducerRegistry.CheckResult.Store.class);
        assertThat(p1b0).isInstanceOf(IdempotentProducerRegistry.CheckResult.Store.class);
        assertThat(p0b1).isInstanceOf(IdempotentProducerRegistry.CheckResult.Store.class);
        assertThat(p1b1).isInstanceOf(IdempotentProducerRegistry.CheckResult.Store.class);
    }

    /**
     * Verifies that sequence numbers wrap correctly at {@link Integer#MAX_VALUE}.
     *
     * <p>After storing a batch at {@code baseSequence=Integer.MAX_VALUE} the next expected
     * sequence is {@code 0} (modular wrap at {@code 2^31}). This test confirms that a
     * subsequent batch at {@code baseSequence=0} is accepted as the continuation, not
     * rejected as out-of-order.</p>
     */
    @Test
    void process_sequenceWrapsAtIntegerMaxValue_shouldAcceptZeroAsNext() {
        // Batch starting at seq=0 with batchCount=MAX_VALUE:
        //   nextSequence(0, MAX_VALUE) = MAX_VALUE
        registry.process(PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO,
            0, Integer.MAX_VALUE, offsetCounter::getAndIncrement);

        // Batch at the wrap point: seq=MAX_VALUE, batchCount=1
        //   nextSequence(MAX_VALUE, 1) = 0
        final var atMax = registry.process(PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO,
            Integer.MAX_VALUE, 1, offsetCounter::getAndIncrement);

        // Batch after the wrap: seq=0 must be accepted as the continuation
        final var afterWrap = registry.process(PRODUCER_ID, EPOCH_ZERO, TOPIC, PARTITION_ZERO,
            0, 1, offsetCounter::getAndIncrement);

        assertThat(atMax).isInstanceOf(IdempotentProducerRegistry.CheckResult.Store.class);
        assertThat(afterWrap).isInstanceOf(IdempotentProducerRegistry.CheckResult.Store.class);
    }
}
