package eu.kafkaesque.junit5;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a topic to be pre-created by the {@link KafkaesqueExtension} before any test code runs.
 *
 * <p>This annotation is intended exclusively as a member of the {@link Kafkaesque#topics()} array
 * and cannot be placed directly on a class or method. Topics declared here are created on the
 * {@link eu.kafkaesque.core.KafkaesqueServer} before the first test method executes, so
 * test code never needs to use the Kafka admin client to set up required topics.</p>
 *
 * <p>Example — pre-create a topic and disable auto-creation so tests are fully explicit:</p>
 * <pre>{@code
 * &#64;Kafkaesque(
 *     autoCreateTopics = false,
 *     topics = &#64;KafkaesqueTopic(name = "orders")
 * )
 * class OrderProcessorTest {
 *
 *     &#64;Test
 *     void shouldConsumeOrder(KafkaesqueServer server) throws Exception {
 *         // "orders" topic already exists — no admin client needed
 *     }
 * }
 * }</pre>
 *
 * <p>Multiple topics can be declared by supplying an array:</p>
 * <pre>{@code
 * &#64;Kafkaesque(topics = {
 *     &#64;KafkaesqueTopic(name = "orders"),
 *     &#64;KafkaesqueTopic(name = "shipments", numPartitions = 3)
 * })
 * }</pre>
 *
 * @see Kafkaesque#topics()
 * @see KafkaesqueExtension
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface KafkaesqueTopic {

    /**
     * The name of the topic to pre-create.
     *
     * @return the topic name
     */
    String name();

    /**
     * The number of partitions to create for the topic.
     *
     * @return the partition count; defaults to {@code 1}
     */
    int numPartitions() default 1;

    /**
     * The replication factor for the topic.
     *
     * @return the replication factor; defaults to {@code 1}
     */
    short replicationFactor() default 1;
}
