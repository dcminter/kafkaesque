package eu.kafkaesque.junit5;

import eu.kafkaesque.core.KafkaesqueServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.io.IOException;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * JUnit 5 extension that starts a {@link KafkaesqueServer} and makes it available for
 * parameter injection into test and lifecycle methods.
 *
 * <p>This extension is registered automatically when the {@link Kafkaesque} annotation is
 * applied to a test class or test method. It is not typically used directly.</p>
 *
 * <p>Server lifetime depends on where {@link Kafkaesque} is placed:</p>
 * <ul>
 *   <li>Class with {@link Kafkaesque.Lifecycle#PER_CLASS} — one server for the whole class.</li>
 *   <li>Class with {@link Kafkaesque.Lifecycle#PER_METHOD} — a fresh server per test method.</li>
 *   <li>Individual method — a dedicated server for that method only, regardless of any
 *       class-level annotation.</li>
 * </ul>
 *
 * <p>Class-scoped and method-scoped servers are stored under distinct keys
 * ({@link #CLASS_SERVER_KEY} and {@link #METHOD_SERVER_KEY}). This prevents JUnit 5's
 * store parent-inheritance from conflating the two when both a class and a method carry
 * {@link Kafkaesque} simultaneously.</p>
 *
 * <p>The extension supports three kinds of parameter injection:</p>
 * <ul>
 *   <li>{@link KafkaesqueServer} — the running server instance</li>
 *   <li>{@link KafkaProducer} annotated with {@link KafkaesqueProducer} — a configured producer
 *       that is automatically closed at the end of the test</li>
 *   <li>{@link KafkaConsumer} annotated with {@link KafkaesqueConsumer} — a configured consumer,
 *       optionally pre-subscribed to topics, that is automatically closed at the end of the test</li>
 * </ul>
 *
 * @see Kafkaesque
 * @see KafkaesqueServer
 * @see KafkaesqueProducer
 * @see KafkaesqueConsumer
 */
@Slf4j
public final class KafkaesqueExtension
        implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback, ParameterResolver {

    /** Namespace used to scope stored extension state. */
    private static final Namespace NAMESPACE = Namespace.create(KafkaesqueExtension.class);

    /**
     * Key under which the class-scoped {@link KafkaesqueServer} is stored.
     * Used by {@link Kafkaesque.Lifecycle#PER_CLASS} and stored in the class extension context.
     */
    private static final String CLASS_SERVER_KEY = "class-server";

    /**
     * Key under which a method-scoped {@link KafkaesqueServer} is stored.
     * Used by {@link Kafkaesque.Lifecycle#PER_METHOD} and method-level annotations,
     * stored in the method extension context. Kept distinct from {@link #CLASS_SERVER_KEY}
     * so that JUnit 5's store parent-inheritance cannot confuse the two.
     */
    private static final String METHOD_SERVER_KEY = "method-server";

    /**
     * Starts a class-scoped {@link KafkaesqueServer} when the class carries {@link Kafkaesque}
     * with {@link Kafkaesque.Lifecycle#PER_CLASS}. Does nothing if the class has no
     * {@link Kafkaesque} annotation (e.g. when the extension was registered via a method annotation).
     *
     * @param context the JUnit 5 extension context for the test class
     * @throws Exception if the server cannot be started
     */
    @Override
    public void beforeAll(final ExtensionContext context) throws Exception {
        if (getClassLifecycle(context) == Kafkaesque.Lifecycle.PER_CLASS) {
            startServer(context, CLASS_SERVER_KEY);
        }
    }

    /**
     * Stops the class-scoped {@link KafkaesqueServer} when the class carries {@link Kafkaesque}
     * with {@link Kafkaesque.Lifecycle#PER_CLASS}. Does nothing if the class has no
     * {@link Kafkaesque} annotation.
     *
     * @param context the JUnit 5 extension context for the test class
     */
    @Override
    public void afterAll(final ExtensionContext context) {
        if (getClassLifecycle(context) == Kafkaesque.Lifecycle.PER_CLASS) {
            stopServer(context, CLASS_SERVER_KEY);
        }
    }

    /**
     * Starts a method-scoped {@link KafkaesqueServer} when the test method itself carries
     * {@link Kafkaesque}, or when the class lifecycle is {@link Kafkaesque.Lifecycle#PER_METHOD}.
     *
     * <p>A server already present under {@link #METHOD_SERVER_KEY} in the method context (placed
     * by an earlier registration of this extension for the same method) is left untouched to
     * avoid starting duplicates. Because method-scoped servers use a distinct key from
     * class-scoped ones, JUnit 5's store parent-inheritance cannot trigger this guard
     * prematurely.</p>
     *
     * @param context the JUnit 5 extension context for the test method
     * @throws Exception if the server cannot be started
     */
    @Override
    public void beforeEach(final ExtensionContext context) throws Exception {
        if (context.getStore(NAMESPACE).get(METHOD_SERVER_KEY) != null) {
            return; // already started by an earlier registration of this extension for this method
        }
        if (hasMethodAnnotation(context) || getClassLifecycle(context) == Kafkaesque.Lifecycle.PER_METHOD) {
            startServer(context, METHOD_SERVER_KEY);
        }
    }

    /**
     * Stops the method-scoped {@link KafkaesqueServer} when the test method itself carries
     * {@link Kafkaesque}, or when the class lifecycle is {@link Kafkaesque.Lifecycle#PER_METHOD}.
     * Calling this a second time for the same context (double-registration scenario) is harmless
     * because {@link #stopServer} is idempotent.
     *
     * @param context the JUnit 5 extension context for the test method
     */
    @Override
    public void afterEach(final ExtensionContext context) {
        if (hasMethodAnnotation(context) || getClassLifecycle(context) == Kafkaesque.Lifecycle.PER_METHOD) {
            stopServer(context, METHOD_SERVER_KEY);
        }
    }

    /**
     * Returns {@code true} when the parameter can be resolved by this extension.
     *
     * <p>Supported parameter types are:</p>
     * <ul>
     *   <li>{@link KafkaesqueServer} or any subtype</li>
     *   <li>{@link KafkaProducer} annotated with {@link KafkaesqueProducer}</li>
     *   <li>{@link KafkaConsumer} annotated with {@link KafkaesqueConsumer}</li>
     * </ul>
     *
     * @param paramCtx the context for the parameter requesting injection
     * @param extCtx   the current JUnit 5 extension context
     * @return {@code true} when the parameter can be resolved
     */
    @Override
    public boolean supportsParameter(final ParameterContext paramCtx, final ExtensionContext extCtx) {
        return KafkaesqueServer.class.isAssignableFrom(paramCtx.getParameter().getType())
            || isAnnotatedProducer(paramCtx)
            || isAnnotatedConsumer(paramCtx);
    }

    /**
     * Resolves the parameter value for the current test.
     *
     * <p>Delegates to {@link #createAndStoreProducer} or {@link #createAndStoreConsumer} for
     * annotated producer and consumer parameters respectively. Falls back to
     * {@link #resolveServer} for {@link KafkaesqueServer} parameters.</p>
     *
     * @param paramCtx the context for the parameter requesting injection
     * @param extCtx   the current JUnit 5 extension context
     * @return the resolved parameter value
     * @throws ParameterResolutionException if a Kafka client cannot be created
     * @throws IllegalStateException        if a {@link KafkaesqueServer} is requested but none is found
     */
    @Override
    public Object resolveParameter(final ParameterContext paramCtx, final ExtensionContext extCtx) {
        try {
            if (isAnnotatedProducer(paramCtx)) {
                return createAndStoreProducer(paramCtx, extCtx);
            }
            if (isAnnotatedConsumer(paramCtx)) {
                return createAndStoreConsumer(paramCtx, extCtx);
            }
        } catch (final IOException e) {
            throw new ParameterResolutionException("Failed to determine Kafka bootstrap address", e);
        }
        return resolveServer(extCtx);
    }

    /**
     * Starts a new {@link KafkaesqueServer}, stores it under {@code key} in the given context,
     * and logs the bound address.
     *
     * @param context the extension context in which to store the server
     * @param key     the store key to use ({@link #CLASS_SERVER_KEY} or {@link #METHOD_SERVER_KEY})
     * @throws Exception if the server cannot be started
     */
    private void startServer(final ExtensionContext context, final String key) throws Exception {
        final var server = new KafkaesqueServer("localhost", 0, resolveAutoCreateTopics(context));
        server.start();
        createTopics(server, context);
        context.getStore(NAMESPACE).put(key, server);
        log.info("Kafkaesque server started on {}", server.getBootstrapServers());
    }

    /**
     * Pre-creates all topics declared via {@link Kafkaesque#topics()} on the given server.
     * Topics from both the class-level and method-level annotations are created.
     *
     * @param server  the server on which to create topics
     * @param context the current extension context
     */
    private void createTopics(final KafkaesqueServer server, final ExtensionContext context) {
        resolveTopics(context).forEach(t -> server.createTopic(t.name(), t.numPartitions(), t.replicationFactor()));
    }

    /**
     * Collects all {@link KafkaesqueTopic} declarations from both the class-level and
     * method-level {@link Kafkaesque} annotations. Class-level topics come first; method-level
     * topics are appended so that per-test extras are created after any shared setup topics.
     *
     * @param context the current extension context
     * @return ordered list of topic declarations to pre-create; never {@code null}
     */
    private List<KafkaesqueTopic> resolveTopics(final ExtensionContext context) {
        final var classTopics = Optional.ofNullable(context.getRequiredTestClass().getAnnotation(Kafkaesque.class))
            .map(Kafkaesque::topics)
            .map(Arrays::asList)
            .orElse(List.of());
        final var methodTopics = context.getTestMethod()
            .map(m -> m.getAnnotation(Kafkaesque.class))
            .map(Kafkaesque::topics)
            .map(Arrays::asList)
            .orElse(List.of());
        return Stream.concat(classTopics.stream(), methodTopics.stream()).toList();
    }

    /**
     * Resolves the {@code autoCreateTopics} flag from the nearest {@link Kafkaesque} annotation.
     *
     * <p>Checks the test method first, then the test class. Returns {@code true} if no annotation
     * is found (preserving the default open behaviour).</p>
     *
     * @param context the current extension context
     * @return {@code true} if auto-topic-creation should be enabled
     */
    private boolean resolveAutoCreateTopics(final ExtensionContext context) {
        return context.getTestMethod()
            .map(m -> m.getAnnotation(Kafkaesque.class))
            .map(Kafkaesque::autoCreateTopics)
            .orElseGet(() -> {
                final var annotation = context.getRequiredTestClass().getAnnotation(Kafkaesque.class);
                return annotation == null || annotation.autoCreateTopics();
            });
    }

    /**
     * Removes and stops the {@link KafkaesqueServer} stored under {@code key} in the given
     * context. Safe to call when no server is stored (idempotent).
     *
     * @param context the extension context from which to remove the server
     * @param key     the store key to remove ({@link #CLASS_SERVER_KEY} or {@link #METHOD_SERVER_KEY})
     */
    private void stopServer(final ExtensionContext context, final String key) {
        final var server = context.getStore(NAMESPACE).remove(key, KafkaesqueServer.class);
        if (server != null) {
            server.close();
            log.info("Kafkaesque server stopped");
        }
    }

    /**
     * Locates the active {@link KafkaesqueServer} for the current test.
     *
     * <p>A method-scoped server is returned first when present. If none is found, the
     * class-scoped server is returned via JUnit 5's store parent-inheritance.</p>
     *
     * @param extCtx the current JUnit 5 extension context
     * @return the active {@link KafkaesqueServer}
     * @throws IllegalStateException if no server is found
     */
    private KafkaesqueServer resolveServer(final ExtensionContext extCtx) {
        final var methodServer = extCtx.getStore(NAMESPACE).get(METHOD_SERVER_KEY, KafkaesqueServer.class);
        if (methodServer != null) {
            return methodServer;
        }
        final var classServer = extCtx.getStore(NAMESPACE).get(CLASS_SERVER_KEY, KafkaesqueServer.class);
        if (classServer != null) {
            return classServer;
        }
        throw new IllegalStateException(
            "KafkaesqueServer not found — ensure @Kafkaesque is applied to the test class or method");
    }

    /**
     * Creates a {@link KafkaProducer} from the {@link KafkaesqueProducer} annotation on the
     * given parameter, stores it for auto-close, and returns it.
     *
     * @param paramCtx the parameter context bearing the {@link KafkaesqueProducer} annotation
     * @param extCtx   the current JUnit 5 extension context
     * @return a configured, ready-to-use {@link KafkaProducer}
     * @throws IOException if the server's bootstrap address cannot be determined
     */
    private KafkaProducer<?, ?> createAndStoreProducer(
            final ParameterContext paramCtx, final ExtensionContext extCtx) throws IOException {
        final var annotation = paramCtx.findAnnotation(KafkaesqueProducer.class).orElseThrow();
        final var bootstrapServers = resolveServer(extCtx).getBootstrapServers();
        final var producer = new KafkaProducer<>(buildProducerProperties(annotation, bootstrapServers));
        extCtx.getStore(NAMESPACE).put(
            "producer-" + paramCtx.getIndex(),
            (ExtensionContext.Store.CloseableResource) producer::close);
        return producer;
    }

    /**
     * Builds the {@link Properties} for a {@link KafkaProducer} from the given annotation.
     *
     * @param annotation       the {@link KafkaesqueProducer} annotation to read configuration from
     * @param bootstrapServers the broker address string
     * @return a fully populated {@link Properties} instance
     */
    private Properties buildProducerProperties(
            final KafkaesqueProducer annotation, final String bootstrapServers) {
        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, annotation.keySerializer().getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, annotation.valueSerializer().getName());
        props.put(ProducerConfig.ACKS_CONFIG, annotation.acks());
        props.put(ProducerConfig.RETRIES_CONFIG, annotation.retries());
        if (annotation.idempotent()) {
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            if (annotation.retries() == 0) {
                // Kafka requires retries > 0 for idempotent producers; apply a safe default
                // when the user has not overridden the zero-valued annotation default.
                props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
            }
        }
        if (!annotation.transactionalId().isEmpty()) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, annotation.transactionalId());
        }
        return props;
    }

    /**
     * Creates a {@link KafkaConsumer} from the {@link KafkaesqueConsumer} annotation on the
     * given parameter, optionally subscribes it to the declared topics, stores it for
     * auto-close, and returns it.
     *
     * @param paramCtx the parameter context bearing the {@link KafkaesqueConsumer} annotation
     * @param extCtx   the current JUnit 5 extension context
     * @return a configured, optionally subscribed {@link KafkaConsumer}
     * @throws IOException if the server's bootstrap address cannot be determined
     */
    private KafkaConsumer<?, ?> createAndStoreConsumer(
            final ParameterContext paramCtx, final ExtensionContext extCtx) throws IOException {
        final var annotation = paramCtx.findAnnotation(KafkaesqueConsumer.class).orElseThrow();
        final var bootstrapServers = resolveServer(extCtx).getBootstrapServers();
        final var consumer = new KafkaConsumer<>(buildConsumerProperties(annotation, bootstrapServers));
        if (annotation.topics().length > 0) {
            consumer.subscribe(List.of(annotation.topics()));
        }
        extCtx.getStore(NAMESPACE).put(
            "consumer-" + paramCtx.getIndex(),
            (ExtensionContext.Store.CloseableResource) consumer::close);
        return consumer;
    }

    /**
     * Builds the {@link Properties} for a {@link KafkaConsumer} from the given annotation.
     *
     * @param annotation       the {@link KafkaesqueConsumer} annotation to read configuration from
     * @param bootstrapServers the broker address string
     * @return a fully populated {@link Properties} instance
     */
    private Properties buildConsumerProperties(
            final KafkaesqueConsumer annotation, final String bootstrapServers) {
        final var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
            annotation.groupId().isEmpty() ? "test-group-" + UUID.randomUUID() : annotation.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, annotation.keyDeserializer().getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, annotation.valueDeserializer().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, annotation.autoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, annotation.isolationLevel().toString().toLowerCase());
        return props;
    }

    /**
     * Returns the {@link Kafkaesque.Lifecycle} configured on the test class, or {@code null}
     * if the class carries no {@link Kafkaesque} annotation (e.g. when the extension was
     * registered only via a method annotation).
     *
     * @param context the current extension context
     * @return the configured class lifecycle, or {@code null} if not present
     */
    private Kafkaesque.Lifecycle getClassLifecycle(final ExtensionContext context) {
        final var annotation = context.getRequiredTestClass().getAnnotation(Kafkaesque.class);
        return annotation != null ? annotation.lifecycle() : null;
    }

    /**
     * Returns {@code true} if the current test method is directly annotated with
     * {@link Kafkaesque}.
     *
     * @param context the current extension context
     * @return {@code true} when the test method carries {@link Kafkaesque}
     */
    private boolean hasMethodAnnotation(final ExtensionContext context) {
        return context.getTestMethod()
            .map(m -> m.isAnnotationPresent(Kafkaesque.class))
            .orElse(false);
    }

    /**
     * Returns {@code true} if the parameter is a {@link KafkaProducer} annotated with
     * {@link KafkaesqueProducer}.
     *
     * @param paramCtx the parameter context to inspect
     * @return {@code true} when the parameter should be resolved as an injected producer
     */
    private boolean isAnnotatedProducer(final ParameterContext paramCtx) {
        return KafkaProducer.class.isAssignableFrom(paramCtx.getParameter().getType())
            && paramCtx.isAnnotated(KafkaesqueProducer.class);
    }

    /**
     * Returns {@code true} if the parameter is a {@link KafkaConsumer} annotated with
     * {@link KafkaesqueConsumer}.
     *
     * @param paramCtx the parameter context to inspect
     * @return {@code true} when the parameter should be resolved as an injected consumer
     */
    private boolean isAnnotatedConsumer(final ParameterContext paramCtx) {
        return KafkaConsumer.class.isAssignableFrom(paramCtx.getParameter().getType())
            && paramCtx.isAnnotated(KafkaesqueConsumer.class);
    }
}
