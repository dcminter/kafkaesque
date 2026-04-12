package eu.kafkaesque.it;

import org.junit.jupiter.api.Nested;

/**
 * Abstract aggregator that composes all admin-related Kafka behavior test suites
 * into a single class via {@link Nested} inner classes.
 *
 * <p>This aggregator covers tests that require {@code kafka-clients} 3.5 or later,
 * because they reference admin API classes ({@code AlterConfigOp}, {@code OffsetSpec},
 * {@code TransactionListing}, {@code PatternType}, etc.) that were introduced in
 * later 2.x/3.x releases.</p>
 *
 * <p>Subclasses must implement {@link #getBootstrapServers()} to supply the broker
 * address. Any backend lifecycle (start/stop) must be managed in
 * {@code @BeforeAll}/{@code @AfterAll} methods.</p>
 *
 * @see AbstractAdminBehaviorIT
 * @see AbstractAclBehaviorIT
 * @see AbstractTransactionAdminBehaviorIT
 * @see AbstractConsumerGroupAdminBehaviorIT
 */
abstract class AbstractKafkaAdminBehaviorIT {

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Runs the admin-client behavior tests against this backend.
     */
    @Nested
    class AdminBehavior extends AbstractAdminBehaviorIT {
        @Override
        protected String getBootstrapServers() throws Exception {
            return AbstractKafkaAdminBehaviorIT.this.getBootstrapServers();
        }
    }

    /**
     * Runs the ACL behavior tests against this backend.
     */
    @Nested
    class AclBehavior extends AbstractAclBehaviorIT {
        @Override
        protected String getBootstrapServers() throws Exception {
            return AbstractKafkaAdminBehaviorIT.this.getBootstrapServers();
        }
    }

    /**
     * Runs the consumer group admin behavior tests against this backend.
     */
    @Nested
    class ConsumerGroupAdminBehavior extends AbstractConsumerGroupAdminBehaviorIT {
        @Override
        protected String getBootstrapServers() throws Exception {
            return AbstractKafkaAdminBehaviorIT.this.getBootstrapServers();
        }
    }

    /**
     * Runs the transaction admin behavior tests against this backend.
     */
    @Nested
    class TransactionAdminBehavior extends AbstractTransactionAdminBehaviorIT {
        @Override
        protected String getBootstrapServers() throws Exception {
            return AbstractKafkaAdminBehaviorIT.this.getBootstrapServers();
        }
    }
}
