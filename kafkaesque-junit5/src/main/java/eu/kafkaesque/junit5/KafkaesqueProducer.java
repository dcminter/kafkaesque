package eu.kafkaesque.junit5;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a test method parameter of type {@link org.apache.kafka.clients.producer.KafkaProducer}
 * for injection by the {@link KafkaesqueExtension}.
 *
 * <p>When applied to a {@code KafkaProducer} parameter, the extension creates and injects a
 * fully configured producer connected to the running {@link eu.kafkaesque.core.KafkaesqueServer}.
 * The bootstrap servers are derived automatically from the active server. The producer is
 * automatically closed at the end of the test method.</p>
 *
 * <p>Key and value serializers default to {@link StringSerializer} so most tests need no
 * extra attributes:</p>
 * <pre>{@code
 * &#64;Test
 * void shouldProduce(&#64;KafkaesqueProducer KafkaProducer<String, String> producer) throws Exception {
 *     producer.send(new ProducerRecord<>("topic", "key", "value")).get();
 * }
 * }</pre>
 *
 * <p>Custom serializers can be specified explicitly when the domain type requires them:</p>
 * <pre>{@code
 * &#64;Test
 * void shouldProduceFoo(
 *         &#64;KafkaesqueProducer(valueSerializer = FooSerializer.class)
 *         KafkaProducer<String, Foo> producer) throws Exception { ... }
 * }</pre>
 *
 * <p>Transactional producers can be configured via {@link #transactionalId()}:</p>
 * <pre>{@code
 * &#64;Test
 * void shouldCommitTransaction(
 *         &#64;KafkaesqueProducer(transactionalId = "tx-1", idempotent = true)
 *         KafkaProducer<String, String> producer) throws Exception {
 *     producer.initTransactions();
 *     producer.beginTransaction();
 *     producer.send(new ProducerRecord<>("topic", "key", "value")).get();
 *     producer.commitTransaction();
 * }
 * }</pre>
 *
 * @see KafkaesqueConsumer
 * @see KafkaesqueExtension
 * @see Kafkaesque
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface KafkaesqueProducer {

    /**
     * The serializer class for record keys.
     *
     * @return the key serializer class; defaults to {@link StringSerializer}
     */
    Class<? extends Serializer<?>> keySerializer() default StringSerializer.class;

    /**
     * The serializer class for record values.
     *
     * @return the value serializer class; defaults to {@link StringSerializer}
     */
    Class<? extends Serializer<?>> valueSerializer() default StringSerializer.class;

    /**
     * The number of acknowledgements required before a produce request is considered complete.
     *
     * @return the acknowledgement mode; defaults to {@code "all"}
     */
    String acks() default "all";

    /**
     * The number of times the producer will retry a failed send before giving up.
     *
     * @return the retry count; defaults to {@code 0}
     */
    int retries() default 0;

    /**
     * Whether to enable idempotent production.
     *
     * <p>When {@code true}, the producer is configured with {@code enable.idempotence=true},
     * ensuring exactly-once delivery within a single session. Must be {@code true} when
     * {@link #transactionalId()} is non-empty.</p>
     *
     * @return {@code true} to enable idempotence; defaults to {@code false}
     */
    boolean idempotent() default false;

    /**
     * The transactional ID for this producer.
     *
     * <p>An empty string (the default) creates a non-transactional producer. A non-empty value
     * enables transactional semantics — the producer will be configured with
     * {@code transactional.id} set to this value. {@link #idempotent()} should also be
     * {@code true} when a transactional ID is set.</p>
     *
     * @return the transactional ID; empty string means non-transactional
     */
    String transactionalId() default "";
}
