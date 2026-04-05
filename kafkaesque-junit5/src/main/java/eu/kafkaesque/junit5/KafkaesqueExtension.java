package eu.kafkaesque.junit5;

import eu.kafkaesque.core.KafkaesqueServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

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
 * <p>To receive the server in a test or lifecycle method, declare a parameter of type
 * {@link KafkaesqueServer}:</p>
 * <pre>{@code
 * &#64;Test
 * void myTest(KafkaesqueServer server) throws Exception {
 *     String bootstrapServers = server.getBootstrapServers();
 * }
 * }</pre>
 *
 * @see Kafkaesque
 * @see KafkaesqueServer
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
     * Returns {@code true} if the parameter type is {@link KafkaesqueServer} or a subtype.
     *
     * @param paramCtx the context for the parameter requesting injection
     * @param extCtx   the current JUnit 5 extension context
     * @return {@code true} when the parameter can be resolved as a {@link KafkaesqueServer}
     */
    @Override
    public boolean supportsParameter(final ParameterContext paramCtx, final ExtensionContext extCtx) {
        return KafkaesqueServer.class.isAssignableFrom(paramCtx.getParameter().getType());
    }

    /**
     * Returns the {@link KafkaesqueServer} for the current test.
     *
     * <p>A method-scoped server (stored under {@link #METHOD_SERVER_KEY}) is returned first
     * when present — this gives method-annotated tests their own isolated instance even when
     * the class also carries {@link Kafkaesque}. If no method-scoped server is found, the
     * class-scoped server (stored under {@link #CLASS_SERVER_KEY}) is returned; JUnit 5's
     * store parent-inheritance locates it in the enclosing class context automatically.</p>
     *
     * @param paramCtx the context for the parameter requesting injection
     * @param extCtx   the current JUnit 5 extension context
     * @return the running {@link KafkaesqueServer} for this test
     * @throws IllegalStateException if no server is found — ensure {@link Kafkaesque}
     *                               is applied to the test class or method
     */
    @Override
    public Object resolveParameter(final ParameterContext paramCtx, final ExtensionContext extCtx) {
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
        context.getStore(NAMESPACE).put(key, server);
        log.info("Kafkaesque server started on {}", server.getBootstrapServers());
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
}
