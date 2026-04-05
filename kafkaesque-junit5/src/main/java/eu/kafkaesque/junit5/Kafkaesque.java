package eu.kafkaesque.junit5;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Composed JUnit 5 annotation that registers the {@link KafkaesqueExtension} for a test class
 * or an individual test method.
 *
 * <p>When applied to a <em>class</em>, the {@link #lifecycle()} attribute controls when the
 * server is started and stopped:</p>
 * <ul>
 *   <li>{@link Lifecycle#PER_CLASS} (default) — one server per test class, shared by all tests</li>
 *   <li>{@link Lifecycle#PER_METHOD} — a fresh server for each test method</li>
 * </ul>
 *
 * <p>When applied to an individual <em>test method</em>, that method always receives its own
 * dedicated {@link eu.kafkaesque.core.KafkaesqueServer} instance, independent of any
 * class-level annotation. The {@link #lifecycle()} attribute has no effect when the annotation
 * is on a method.</p>
 *
 * <p>Example — shared server for the whole class (default):</p>
 * <pre>{@code
 * &#64;Kafkaesque
 * class MyKafkaTest {
 *
 *     &#64;Test
 *     void shouldProduceAndConsume(KafkaesqueServer server) throws Exception {
 *         String bootstrapServers = server.getBootstrapServers();
 *         // configure Kafka clients with bootstrapServers ...
 *     }
 * }
 * }</pre>
 *
 * <p>Example — dedicated server for one method within a class that shares a server:</p>
 * <pre>{@code
 * &#64;Kafkaesque
 * class MyKafkaTest {
 *
 *     &#64;Test
 *     void usesSharedServer(KafkaesqueServer server) throws Exception { ... }
 *
 *     &#64;Kafkaesque
 *     &#64;Test
 *     void hasItsOwnServer(KafkaesqueServer server) throws Exception { ... }
 * }
 * }</pre>
 *
 * @see KafkaesqueExtension
 * @see eu.kafkaesque.core.KafkaesqueServer
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ExtendWith(KafkaesqueExtension.class)
public @interface Kafkaesque {

    /**
     * Controls when the {@link eu.kafkaesque.core.KafkaesqueServer} is started and stopped.
     * Only meaningful when the annotation is placed on a class; ignored on individual methods.
     *
     * @return the server lifecycle; defaults to {@link Lifecycle#PER_CLASS}
     */
    Lifecycle lifecycle() default Lifecycle.PER_CLASS;

    /**
     * Defines when the Kafkaesque server is started and stopped relative to test execution.
     * Only relevant when {@link Kafkaesque} is applied at the class level.
     *
     * @see Kafkaesque#lifecycle()
     */
    enum Lifecycle {

        /**
         * One server is started before the first test in the class and stopped after the last.
         * All tests in the class share the same server instance.
         */
        PER_CLASS,

        /**
         * A fresh server is started before each individual test method and stopped after it.
         * Tests are fully isolated from one another.
         */
        PER_METHOD
    }
}
