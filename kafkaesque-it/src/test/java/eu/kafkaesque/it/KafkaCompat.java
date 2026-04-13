package eu.kafkaesque.it;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.lang.reflect.Method;
import java.time.Duration;

/**
 * Provides a version-compatible {@code poll} method for {@link KafkaConsumer}
 * that works across Kafka client versions 1.x through 4.x.
 *
 * <p>Kafka 1.x only has {@code poll(long)}, Kafka 2.x&ndash;3.x has both
 * {@code poll(long)} and {@code poll(Duration)}, and Kafka 4.x removed
 * {@code poll(long)}. This utility uses reflection to call whichever
 * overload is available at runtime.</p>
 */
final class KafkaCompat {

    private static final Method POLL_METHOD;

    static {
        Method method;
        try {
            method = KafkaConsumer.class.getMethod("poll", Duration.class);
        } catch (final NoSuchMethodException e) {
            try {
                method = KafkaConsumer.class.getMethod("poll", long.class);
            } catch (final NoSuchMethodException ex) {
                throw new IllegalStateException("No compatible poll method found on KafkaConsumer", ex);
            }
        }
        POLL_METHOD = method;
    }

    private KafkaCompat() {
    }

    /**
     * Polls the given consumer for records, using whichever {@code poll} overload
     * is available in the current Kafka client version.
     *
     * @param consumer   the consumer to poll
     * @param timeoutMs  the poll timeout in milliseconds
     * @param <K>        the key type
     * @param <V>        the value type
     * @return the consumer records returned by the poll
     */
    @SuppressWarnings("unchecked")
    static <K, V> ConsumerRecords<K, V> poll(final KafkaConsumer<K, V> consumer, final long timeoutMs) {
        try {
            if (POLL_METHOD.getParameterTypes()[0] == Duration.class) {
                return (ConsumerRecords<K, V>) POLL_METHOD.invoke(consumer, Duration.ofMillis(timeoutMs));
            } else {
                return (ConsumerRecords<K, V>) POLL_METHOD.invoke(consumer, timeoutMs);
            }
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to invoke KafkaConsumer.poll", e);
        }
    }
}
