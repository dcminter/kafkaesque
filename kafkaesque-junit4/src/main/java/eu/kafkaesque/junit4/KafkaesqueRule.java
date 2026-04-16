package eu.kafkaesque.junit4;

import eu.kafkaesque.core.KafkaesqueServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static java.util.Arrays.asList;

/**
 * JUnit 4 rule that starts a {@link KafkaesqueServer} and provides factory methods for
 * creating Kafka producers and consumers connected to it.
 *
 * <p>Use as a {@link org.junit.Rule @Rule} for a fresh server per test method, or as a
 * {@link org.junit.ClassRule @ClassRule} for a shared server across all test methods in a
 * class.</p>
 *
 * <p>Example — per-method server (default settings):</p>
 * <pre>{@code
 * public class MyKafkaTest {
 *     &#64;Rule
 *     public KafkaesqueRule kafka = new KafkaesqueRule();
 *
 *     &#64;Test
 *     public void shouldProduce() throws Exception {
 *         KafkaProducer<String, String> producer = kafka.createProducer();
 *         producer.send(new ProducerRecord<>("topic", "key", "value")).get();
 *     }
 * }
 * }</pre>
 *
 * <p>Example — shared server with pre-created topics:</p>
 * <pre>{@code
 * public class MyKafkaTest {
 *     &#64;ClassRule
 *     public static KafkaesqueRule kafka = KafkaesqueRule.builder()
 *         .autoCreateTopics(false)
 *         .topic("orders")
 *         .topic("shipments", 3)
 *         .build();
 *
 *     &#64;Test
 *     public void shouldProduceToOrders() throws Exception {
 *         KafkaProducer<String, String> producer = kafka.createProducer();
 *         producer.send(new ProducerRecord<>("orders", "key", "value")).get();
 *     }
 * }
 * }</pre>
 *
 * <p>All producers and consumers created via the factory methods are automatically closed
 * when the rule tears down (i.e. after each test for {@code @Rule}, or after the class for
 * {@code @ClassRule}).</p>
 *
 * <p>This rule also works with the JUnit 5 Vintage engine, allowing JUnit 4 tests to run
 * alongside JUnit 5 tests in the same project.</p>
 *
 * @see TopicDefinition
 * @see KafkaesqueServer
 */
@Slf4j
public final class KafkaesqueRule extends ExternalResource {

    /** The port number to bind the server to; {@code 0} for an OS-assigned ephemeral port. */
    private final int port;

    /** Whether producers may auto-create unknown topics. */
    private final boolean autoCreateTopics;

    /** Topics to pre-create when the server starts. */
    private final List<TopicDefinition> topics;

    /** Resources (producers/consumers) to close on teardown. */
    private final List<AutoCloseable> trackedResources = new ArrayList<>();

    /** The running server; {@code null} before {@link #before()} and after {@link #after()}. */
    private KafkaesqueServer server;

    /**
     * Creates a rule with default settings: ephemeral port, auto-create topics enabled,
     * no pre-created topics.
     */
    public KafkaesqueRule() {
        this(0, true, List.of());
    }

    /**
     * Creates a rule with the given configuration.
     *
     * @param port             the port number to bind to; {@code 0} for an OS-assigned ephemeral port
     * @param autoCreateTopics whether producers may auto-create unknown topics
     * @param topics           topics to pre-create when the server starts
     */
    private KafkaesqueRule(final int port, final boolean autoCreateTopics, final List<TopicDefinition> topics) {
        this.port = port;
        this.autoCreateTopics = autoCreateTopics;
        this.topics = topics;
    }

    /**
     * Returns a new builder for configuring the rule.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Starts the {@link KafkaesqueServer} and pre-creates any configured topics.
     * Called automatically by JUnit 4 before each test ({@code @Rule}) or before
     * the first test in the class ({@code @ClassRule}).
     *
     * @throws Exception if the server cannot be started
     */
    @Override
    protected void before() throws Exception {
        server = new KafkaesqueServer("localhost", port, autoCreateTopics);
        server.start();
        topics.forEach(t -> server.createTopic(t.name(), t.numPartitions(), t.replicationFactor()));
        log.info("Kafkaesque server started on {}", server.getBootstrapServers());
    }

    /**
     * Closes all tracked producers and consumers, then stops the {@link KafkaesqueServer}.
     * Called automatically by JUnit 4 after each test ({@code @Rule}) or after the last
     * test in the class ({@code @ClassRule}).
     */
    @Override
    protected void after() {
        closeTrackedResources();
        if (server != null) {
            server.close();
            log.info("Kafkaesque server stopped");
            server = null;
        }
    }

    /**
     * Returns the running {@link KafkaesqueServer}.
     *
     * @return the server instance
     * @throws IllegalStateException if the server has not been started
     */
    public KafkaesqueServer getServer() {
        if (server == null) {
            throw new IllegalStateException(
                "KafkaesqueServer not started — ensure KafkaesqueRule is used as @Rule or @ClassRule");
        }
        return server;
    }

    /**
     * Returns the bootstrap servers connection string for the running server.
     *
     * @return the bootstrap servers string (e.g. {@code "localhost:12345"})
     * @throws IllegalStateException if the server has not been started
     * @throws IOException           if the server address cannot be determined
     */
    public String getBootstrapServers() throws IOException {
        return getServer().getBootstrapServers();
    }

    /**
     * Creates a {@link KafkaProducer} with default settings ({@link StringSerializer} for
     * keys and values, acks=all, retries=0). The producer is automatically closed when the
     * rule tears down.
     *
     * @return a configured, ready-to-use producer
     * @throws IOException if the server address cannot be determined
     */
    public KafkaProducer<String, String> createProducer() throws IOException {
        return createProducer(StringSerializer.class, StringSerializer.class);
    }

    /**
     * Creates a {@link KafkaProducer} with the given serializers and default settings
     * (acks=all, retries=0). The producer is automatically closed when the rule tears down.
     *
     * @param keySerializer   the serializer class for record keys
     * @param valueSerializer the serializer class for record values
     * @param <K>             the key type
     * @param <V>             the value type
     * @return a configured, ready-to-use producer
     * @throws IOException if the server address cannot be determined
     */
    public <K, V> KafkaProducer<K, V> createProducer(
            final Class<? extends Serializer<K>> keySerializer,
            final Class<? extends Serializer<V>> valueSerializer) throws IOException {
        final var props = buildDefaultProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
        return createAndTrackProducer(props);
    }

    /**
     * Creates a {@link KafkaProducer} from the given properties. The
     * {@code bootstrap.servers} property is set automatically from the running server.
     * The producer is automatically closed when the rule tears down.
     *
     * @param properties the producer configuration properties
     * @param <K>        the key type
     * @param <V>        the value type
     * @return a configured, ready-to-use producer
     * @throws IOException if the server address cannot be determined
     */
    public <K, V> KafkaProducer<K, V> createProducer(final Properties properties) throws IOException {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        return createAndTrackProducer(properties);
    }

    /**
     * Creates a {@link KafkaConsumer} with default settings ({@link StringDeserializer} for
     * keys and values, random group ID, auto-offset-reset=earliest). If topics are specified
     * the consumer is pre-subscribed to them. The consumer is automatically closed when the
     * rule tears down.
     *
     * @param topics topics to subscribe to; pass none for manual subscription
     * @return a configured, optionally subscribed consumer
     * @throws IOException if the server address cannot be determined
     */
    public KafkaConsumer<String, String> createConsumer(final String... topics) throws IOException {
        return createConsumer(StringDeserializer.class, StringDeserializer.class, topics);
    }

    /**
     * Creates a {@link KafkaConsumer} with the given deserializers and default settings
     * (random group ID, auto-offset-reset=earliest). If topics are specified the consumer
     * is pre-subscribed to them. The consumer is automatically closed when the rule tears
     * down.
     *
     * @param keyDeserializer   the deserializer class for record keys
     * @param valueDeserializer the deserializer class for record values
     * @param topics            topics to subscribe to; pass none for manual subscription
     * @param <K>               the key type
     * @param <V>               the value type
     * @return a configured, optionally subscribed consumer
     * @throws IOException if the server address cannot be determined
     */
    public <K, V> KafkaConsumer<K, V> createConsumer(
            final Class<? extends Deserializer<K>> keyDeserializer,
            final Class<? extends Deserializer<V>> valueDeserializer,
            final String... topics) throws IOException {
        final var props = buildDefaultConsumerProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        return createAndTrackConsumer(props, topics);
    }

    /**
     * Creates a {@link KafkaConsumer} from the given properties. The
     * {@code bootstrap.servers} property is set automatically from the running server.
     * The consumer is <em>not</em> subscribed to any topics. The consumer is automatically
     * closed when the rule tears down.
     *
     * @param properties the consumer configuration properties
     * @param <K>        the key type
     * @param <V>        the value type
     * @return a configured consumer
     * @throws IOException if the server address cannot be determined
     */
    public <K, V> KafkaConsumer<K, V> createConsumer(final Properties properties) throws IOException {
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        return createAndTrackConsumer(properties);
    }

    /**
     * Builds the default {@link Properties} for a producer (without serializer config).
     *
     * @return properties with bootstrap servers, acks, and retries configured
     * @throws IOException if the server address cannot be determined
     */
    private Properties buildDefaultProducerProperties() throws IOException {
        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        return props;
    }

    /**
     * Builds the default {@link Properties} for a consumer (without deserializer config).
     *
     * @return properties with bootstrap servers, group ID, offset reset, and auto-commit
     * @throws IOException if the server address cannot be determined
     */
    private Properties buildDefaultConsumerProperties() throws IOException {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        return props;
    }

    /**
     * Creates a {@link KafkaProducer} from the given properties and registers it for
     * automatic cleanup.
     *
     * @param props the fully populated producer properties
     * @param <K>   the key type
     * @param <V>   the value type
     * @return the created producer
     */
    private <K, V> KafkaProducer<K, V> createAndTrackProducer(final Properties props) {
        final var producer = new KafkaProducer<K, V>(props);
        trackedResources.add(producer);
        return producer;
    }

    /**
     * Creates a {@link KafkaConsumer} from the given properties, optionally subscribes it,
     * and registers it for automatic cleanup.
     *
     * @param props  the fully populated consumer properties
     * @param topics topics to subscribe to; empty for no subscription
     * @param <K>    the key type
     * @param <V>    the value type
     * @return the created consumer
     */
    private <K, V> KafkaConsumer<K, V> createAndTrackConsumer(
            final Properties props, final String... topics) {
        final var consumer = new KafkaConsumer<K, V>(props);
        if (topics.length > 0) {
            consumer.subscribe(List.of(topics));
        }
        trackedResources.add(consumer);
        return consumer;
    }

    /**
     * Closes all tracked producers and consumers in reverse creation order, logging
     * warnings for any that fail to close cleanly.
     */
    private void closeTrackedResources() {
        final var reversed = new ArrayList<>(trackedResources);
        Collections.reverse(reversed);
        for (final var resource : reversed) {
            try {
                resource.close();
            }
            catch (final Exception e) {
                log.warn("Failed to close tracked resource: {}", resource, e);
            }
        }
        trackedResources.clear();
    }

    /**
     * Builder for configuring a {@link KafkaesqueRule} with custom settings.
     *
     * <p>Example:</p>
     * <pre>{@code
     * KafkaesqueRule.builder()
     *     .autoCreateTopics(false)
     *     .topic("orders")
     *     .topic("shipments", 3)
     *     .build();
     * }</pre>
     *
     * @see KafkaesqueRule
     */
    public static final class Builder {

        /** The port number to bind to; {@code 0} for an OS-assigned ephemeral port. */
        private int port;

        /** Whether producers may auto-create unknown topics. */
        private boolean autoCreateTopics = true;

        /** Topics to pre-create when the server starts. */
        private final List<TopicDefinition> topics = new ArrayList<>();

        /**
         * Creates a new builder with default settings.
         */
        Builder() {
        }

        /**
         * Sets the port number the server should listen on.
         *
         * <p>When set to {@code 0} (the default), the operating system assigns an available
         * ephemeral port automatically. Set to a specific positive value to bind the server
         * to an explicit port.</p>
         *
         * @param port the port number; {@code 0} for an OS-assigned ephemeral port
         * @return this builder
         */
        public Builder port(final int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets whether producers may auto-create unknown topics.
         *
         * @param autoCreateTopics {@code true} to enable (default); {@code false} to disable
         * @return this builder
         */
        public Builder autoCreateTopics(final boolean autoCreateTopics) {
            this.autoCreateTopics = autoCreateTopics;
            return this;
        }

        /**
         * Adds a topic with a single partition and replication factor of 1.
         *
         * @param name the topic name
         * @return this builder
         */
        public Builder topic(final String name) {
            topics.add(new TopicDefinition(name));
            return this;
        }

        /**
         * Adds a topic with the given partition count and replication factor of 1.
         *
         * @param name          the topic name
         * @param numPartitions the number of partitions
         * @return this builder
         */
        public Builder topic(final String name, final int numPartitions) {
            topics.add(new TopicDefinition(name, numPartitions));
            return this;
        }

        /**
         * Adds one or more topics with full control over their configuration.
         *
         * @param definitions the topic definitions to add
         * @return this builder
         */
        public Builder topics(final TopicDefinition... definitions) {
            topics.addAll(asList(definitions));
            return this;
        }

        /**
         * Builds a new {@link KafkaesqueRule} with the configured settings.
         *
         * @return a new rule instance
         */
        public KafkaesqueRule build() {
            return new KafkaesqueRule(port, autoCreateTopics, List.copyOf(topics));
        }
    }
}
