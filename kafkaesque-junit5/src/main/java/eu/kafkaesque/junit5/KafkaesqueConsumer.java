package eu.kafkaesque.junit5;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a test method parameter of type {@link org.apache.kafka.clients.consumer.KafkaConsumer}
 * for injection by the {@link KafkaesqueExtension}.
 *
 * <p>When applied to a {@code KafkaConsumer} parameter, the extension creates and injects a
 * fully configured consumer connected to the running {@link eu.kafkaesque.core.KafkaesqueServer}.
 * The bootstrap servers are derived automatically from the active server. When {@link #topics()}
 * is non-empty, the consumer is already subscribed to those topics before the test method
 * receives it. The consumer is automatically closed at the end of the test method.</p>
 *
 * <p>Key and value deserializers default to {@link StringDeserializer} and the group ID defaults
 * to a random UUID (test isolation), so most tests need only specify the topics:</p>
 * <pre>{@code
 * &#64;Test
 * void shouldConsume(&#64;KafkaesqueConsumer(topics = "orders") KafkaConsumer<String, String> consumer) {
 *     ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
 *     assertThat(records).isNotEmpty();
 * }
 * }</pre>
 *
 * <p>When no topics are specified the consumer is injected but not subscribed — the test can
 * call {@code subscribe()} or {@code assign()} itself:</p>
 * <pre>{@code
 * &#64;Test
 * void shouldConsumeWithManualSubscribe(
 *         &#64;KafkaesqueConsumer KafkaConsumer<String, String> consumer) {
 *     consumer.subscribe(List.of("my-topic"));
 *     // ...
 * }
 * }</pre>
 *
 * <p>Custom deserializers and isolation level can be specified when needed:</p>
 * <pre>{@code
 * &#64;Test
 * void shouldConsumeCommittedOnly(
 *         &#64;KafkaesqueConsumer(
 *             topics = "orders",
 *             valueDeserializer = FooDeserializer.class,
 *             isolationLevel = READ_COMMITTED)
 *         KafkaConsumer<String, Foo> consumer) { ... }
 * }</pre>
 *
 * @see KafkaesqueProducer
 * @see KafkaesqueExtension
 * @see Kafkaesque
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface KafkaesqueConsumer {

    /**
     * Topics to subscribe to before the test method runs.
     *
     * <p>When non-empty, the extension calls {@code consumer.subscribe(topics)} immediately
     * after creating the consumer, so the test receives an already-subscribed instance.
     * When empty (the default), no subscription is made and the test is responsible for
     * calling {@code subscribe()} or {@code assign()} itself.</p>
     *
     * @return the topics to subscribe to; defaults to none
     */
    String[] topics() default {};

    /**
     * The consumer group ID.
     *
     * <p>An empty string (the default) causes the extension to generate a random UUID-based
     * group ID for each test, ensuring isolation between tests that share a server.</p>
     *
     * @return the group ID; empty string means a random UUID will be used
     */
    String groupId() default "";

    /**
     * The deserializer class for record keys.
     *
     * @return the key deserializer class; defaults to {@link StringDeserializer}
     */
    Class<? extends Deserializer<?>> keyDeserializer() default StringDeserializer.class;

    /**
     * The deserializer class for record values.
     *
     * @return the value deserializer class; defaults to {@link StringDeserializer}
     */
    Class<? extends Deserializer<?>> valueDeserializer() default StringDeserializer.class;

    /**
     * The offset reset strategy when no committed offset exists for the consumer group.
     *
     * @return the auto offset reset policy; defaults to {@code "earliest"}
     */
    String autoOffsetReset() default "earliest";

    /**
     * The transaction isolation level for this consumer.
     *
     * <p>{@link IsolationLevel#READ_UNCOMMITTED} (the default) returns all records regardless
     * of transaction state. {@link IsolationLevel#READ_COMMITTED} returns only records from
     * committed transactions and non-transactional records.</p>
     *
     * @return the isolation level; defaults to {@link IsolationLevel#READ_UNCOMMITTED}
     */
    IsolationLevel isolationLevel() default IsolationLevel.READ_UNCOMMITTED;
}
