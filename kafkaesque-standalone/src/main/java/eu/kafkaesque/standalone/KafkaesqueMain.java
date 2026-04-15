package eu.kafkaesque.standalone;

import eu.kafkaesque.core.KafkaesqueServer;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.CountDownLatch;

import static java.lang.System.getenv;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Runtime.getRuntime;

/**
 * Entry point for running Kafkaesque as a standalone server.
 *
 * <p>This class starts a {@link KafkaesqueServer} that listens on a configurable host and port,
 * making it suitable for running as a Docker container or executable JAR. The server runs until
 * the process is terminated (e.g. via {@code SIGTERM} or {@code SIGINT}).</p>
 *
 * <h2>Configuration</h2>
 * <p>The following environment variables control server behaviour:</p>
 * <ul>
 *   <li>{@code KAFKAESQUE_HOST} &mdash; bind address (default {@code 0.0.0.0})</li>
 *   <li>{@code KAFKAESQUE_PORT} &mdash; listen port (default {@code 9092})</li>
 *   <li>{@code KAFKAESQUE_AUTO_CREATE_TOPICS} &mdash; whether producers may auto-create topics
 *       (default {@code true})</li>
 * </ul>
 *
 * @see KafkaesqueServer
 */
@Slf4j
public final class KafkaesqueMain {

    /** Default bind address for the server. */
    static final String DEFAULT_HOST = "0.0.0.0";

    /** Default listen port for the server. */
    static final int DEFAULT_PORT = 9092;

    private KafkaesqueMain() {
        // utility / entry-point class
    }

    /**
     * Starts the Kafkaesque server and blocks until the process is terminated.
     *
     * @param args command-line arguments (currently unused)
     * @throws Exception if the server fails to start
     */
    public static void main(final String[] args) throws Exception {
        final var host = resolveHost();
        final int port = resolvePort();
        final boolean autoCreate = resolveAutoCreateTopics();

        try (var server = new KafkaesqueServer(host, port, autoCreate)) {
            server.start();
            log.info("Kafkaesque server started on {}", server.getBootstrapServers());

            registerShutdownHook(server);
            awaitTermination();
        }
    }

    /**
     * Reads the bind host from the {@code KAFKAESQUE_HOST} environment variable.
     *
     * @return the configured host, or {@value #DEFAULT_HOST} if unset
     */
    static String resolveHost() {
        final var value = getenv("KAFKAESQUE_HOST");
        return value != null && !value.isBlank() ? value.strip() : DEFAULT_HOST;
    }

    /**
     * Reads the listen port from the {@code KAFKAESQUE_PORT} environment variable.
     *
     * @return the configured port, or {@value #DEFAULT_PORT} if unset
     */
    static int resolvePort() {
        final var value = getenv("KAFKAESQUE_PORT");
        if (value == null || value.isBlank()) {
            return DEFAULT_PORT;
        }
        return parseInt(value.strip());
    }

    /**
     * Reads the auto-create-topics flag from the {@code KAFKAESQUE_AUTO_CREATE_TOPICS}
     * environment variable.
     *
     * @return {@code true} if topics should be auto-created (default), {@code false} otherwise
     */
    static boolean resolveAutoCreateTopics() {
        final var value = getenv("KAFKAESQUE_AUTO_CREATE_TOPICS");
        if (value == null || value.isBlank()) {
            return true;
        }
        return parseBoolean(value.strip());
    }

    private static void registerShutdownHook(final KafkaesqueServer server) {
        getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Kafkaesque server...");
            server.close();
            log.info("Kafkaesque server stopped.");
        }, "kafkaesque-shutdown"));
    }

    private static void awaitTermination() throws InterruptedException {
        new CountDownLatch(1).await();
    }
}
