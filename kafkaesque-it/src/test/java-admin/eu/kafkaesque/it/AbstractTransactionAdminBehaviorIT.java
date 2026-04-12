package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract base class defining the transaction admin integration test suite.
 *
 * <p>Covers listing and describing transactions via {@link AdminClient}.
 * Subclasses must implement {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractTransactionAdminBehaviorIT {

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Verifies that active transactions can be listed via the admin API.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldListTransactionsViaAdminClient() throws Exception {
        // Given
        final String topicName = "list-txn-topic-" + UUID.randomUUID();
        final String txnId = "list-txn-id-" + UUID.randomUUID();
        final var adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            // Start a transaction but don't commit
            try (final var producer = createTransactionalProducer(getBootstrapServers(), txnId)) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topicName, "key", "value")).get();

                // When — list transactions while one is open
                final var txns = adminClient.listTransactions().all().get();
                final var txnIds = txns.stream()
                    .map(TransactionListing::transactionalId)
                    .collect(Collectors.toList());

                // Then
                assertThat(txnIds).contains(txnId);
                log.info("Listed transactions, found: {}", txnId);

                producer.commitTransaction();
            }
        }
    }

    /**
     * Verifies that transaction details can be described via the admin API.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldDescribeTransactionsViaAdminClient() throws Exception {
        // Given
        final String topicName = "describe-txn-topic-" + UUID.randomUUID();
        final String txnId = "describe-txn-id-" + UUID.randomUUID();
        final var adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.createTopics(of(new NewTopic(topicName, 1, (short) 1))).all().get();

            try (final var producer = createTransactionalProducer(getBootstrapServers(), txnId)) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topicName, "key", "value")).get();

                // When
                final var descriptions = adminClient.describeTransactions(of(txnId)).all().get();

                // Then
                assertThat(descriptions).containsKey(txnId);
                final var desc = descriptions.get(txnId);
                assertThat(desc.producerId()).isGreaterThanOrEqualTo(0);

                log.info("Described transaction {}: producerId={}", txnId, desc.producerId());

                producer.commitTransaction();
            }
        }
    }

    /**
     * Creates a transactional producer with the given transactional ID.
     *
     * @param bootstrapServers the broker address
     * @param transactionalId  the transactional ID
     * @return a new transactional producer
     */
    private static KafkaProducer<String, String> createTransactionalProducer(
            final String bootstrapServers, final String transactionalId) {
        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return new KafkaProducer<>(props);
    }
}
