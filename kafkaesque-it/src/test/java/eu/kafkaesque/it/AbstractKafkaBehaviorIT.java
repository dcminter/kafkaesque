package eu.kafkaesque.it;

import org.junit.jupiter.api.Nested;

/**
 * Abstract aggregator that composes all Kafka behavior test suites into a single
 * class via {@link Nested} inner classes.
 *
 * <p>Each test case defined in the behavior classes is run against every concrete
 * subclass, ensuring that both the real Kafka broker (via Testcontainers) and the
 * Kafkaesque mock implementation exhibit the same behavior. Adding a new test to any
 * behavior class automatically exercises it against all backends.</p>
 *
 * <p>Subclasses must implement {@link #getBootstrapServers()} to supply the broker
 * address. Any backend lifecycle (start/stop) must be managed in
 * {@code @BeforeAll}/{@code @AfterAll} methods so the backend is ready before the
 * behavior classes' {@code @BeforeEach} methods call {@link #getBootstrapServers()}.</p>
 *
 * @see AbstractProducerBehaviorIT
 * @see AbstractConsumerBehaviorIT
 * @see AbstractCompressionBehaviorIT
 * @see AbstractCompactionBehaviorIT
 * @see AbstractRetentionBehaviorIT
 * @see AbstractCompactDeleteBehaviorIT
 * @see AbstractTransactionBehaviorIT
 */
abstract class AbstractKafkaBehaviorIT {

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Runs the producer behavior tests against this backend.
     */
    @Nested
    class ProducerBehavior extends AbstractProducerBehaviorIT {
        @Override
        protected String getBootstrapServers() throws Exception {
            return AbstractKafkaBehaviorIT.this.getBootstrapServers();
        }
    }

    /**
     * Runs the consumer and round-trip behavior tests against this backend.
     */
    @Nested
    class ConsumerBehavior extends AbstractConsumerBehaviorIT {
        @Override
        protected String getBootstrapServers() throws Exception {
            return AbstractKafkaBehaviorIT.this.getBootstrapServers();
        }
    }

    /**
     * Runs the compression behavior tests against this backend.
     */
    @Nested
    class CompressionBehavior extends AbstractCompressionBehaviorIT {
        @Override
        protected String getBootstrapServers() throws Exception {
            return AbstractKafkaBehaviorIT.this.getBootstrapServers();
        }
    }

    /**
     * Runs the log-compaction behavior tests against this backend.
     */
    @Nested
    class CompactionBehavior extends AbstractCompactionBehaviorIT {
        @Override
        protected String getBootstrapServers() throws Exception {
            return AbstractKafkaBehaviorIT.this.getBootstrapServers();
        }
    }

    /**
     * Runs the retention-deletion behavior tests against this backend.
     */
    @Nested
    class RetentionBehavior extends AbstractRetentionBehaviorIT {
        @Override
        protected String getBootstrapServers() throws Exception {
            return AbstractKafkaBehaviorIT.this.getBootstrapServers();
        }
    }

    /**
     * Runs the combined compact-and-delete behavior tests against this backend.
     */
    @Nested
    class CompactDeleteBehavior extends AbstractCompactDeleteBehaviorIT {
        @Override
        protected String getBootstrapServers() throws Exception {
            return AbstractKafkaBehaviorIT.this.getBootstrapServers();
        }
    }

    /**
     * Runs the Kafka transaction behavior tests against this backend.
     */
    @Nested
    class TransactionBehavior extends AbstractTransactionBehaviorIT {
        @Override
        protected String getBootstrapServers() throws Exception {
            return AbstractKafkaBehaviorIT.this.getBootstrapServers();
        }
    }

    /**
     * Runs the idempotent producer behavior tests against this backend.
     */
    @Nested
    class IdempotentProducerBehavior extends AbstractIdempotentProducerBehaviorIT {
        @Override
        protected String getBootstrapServers() throws Exception {
            return AbstractKafkaBehaviorIT.this.getBootstrapServers();
        }
    }

}
