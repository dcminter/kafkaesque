package eu.kafkaesque.standalone;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that builds the Kafkaesque Docker image and verifies
 * that the containerised server responds to Kafka protocol requests.
 *
 * <p>The test uses the pre-built fat JAR produced by the maven-shade-plugin
 * (available after the {@code package} phase) and wraps it in a minimal
 * JRE container via Testcontainers' {@link ImageFromDockerfile}.</p>
 *
 * <p>Host networking is used so that the Kafka metadata response (which
 * advertises {@code localhost:<port>}) is directly reachable by the test
 * client. An ephemeral port is chosen to avoid conflicts.</p>
 */
@Slf4j
class KafkaesqueDockerIT {

    private static int kafkaPort;

    private static GenericContainer<?> container;

    /**
     * Finds a free port, builds the container image from the fat JAR, and starts
     * the Kafkaesque container with host networking on the chosen port.
     *
     * @throws IOException if no free port can be found
     */
    @SuppressWarnings("resource")
    @BeforeAll
    static void startContainer() throws IOException {
        kafkaPort = findFreePort();
        final var jarPath = Path.of(System.getProperty("standalone.jar"));

        container = new GenericContainer<>(
                new ImageFromDockerfile()
                        .withFileFromPath("kafkaesque.jar", jarPath)
                        .withDockerfileFromBuilder(builder -> builder
                                .from("eclipse-temurin:25-jre-noble")
                                .copy("kafkaesque.jar", "/app/kafkaesque.jar")
                                .workDir("/app")
                                .entryPoint("java", "-jar", "kafkaesque.jar")
                                .build()))
                .withNetworkMode("host")
                .withEnv("KAFKAESQUE_HOST", "localhost")
                .withEnv("KAFKAESQUE_PORT", String.valueOf(kafkaPort))
                .waitingFor(Wait.forLogMessage(".*Kafkaesque server started.*\\n", 1)
                        .withStartupTimeout(Duration.ofSeconds(30)));

        container.start();
        log.info("Kafkaesque container started on localhost:{}", kafkaPort);
    }

    /**
     * Stops and removes the Kafkaesque container.
     */
    @AfterAll
    static void stopContainer() {
        if (container != null) {
            container.stop();
        }
    }

    /**
     * Verifies that the containerised Kafkaesque server accepts admin API
     * requests by listing topics via {@link AdminClient}.
     *
     * @throws Exception if the admin client call fails
     */
    @Test
    void shouldRespondToAdminRequests() throws Exception {
        final var bootstrap = bootstrapServers();
        log.info("Connecting AdminClient to {}", bootstrap);

        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (var admin = AdminClient.create(props)) {
            final var topics = admin.listTopics().names().get();
            assertThat(topics).isNotNull();
            log.info("AdminClient received topic list: {}", topics);
        }
    }

    /**
     * Verifies end-to-end Kafka protocol support by producing a record to the
     * containerised Kafkaesque server and confirming that the record was accepted.
     *
     * @throws Exception if the produce request fails
     */
    @Test
    void shouldAcceptProducedRecords() throws Exception {
        final var bootstrap = bootstrapServers();
        log.info("Connecting KafkaProducer to {}", bootstrap);

        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (var producer = new KafkaProducer<String, String>(props)) {
            final var metadata = producer.send(
                    new ProducerRecord<>("docker-test-topic", "key-1", "hello from docker"))
                    .get();

            assertThat(metadata.topic()).isEqualTo("docker-test-topic");
            assertThat(metadata.partition()).isGreaterThanOrEqualTo(0);
            assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);
            log.info("Record accepted: topic={}, partition={}, offset={}",
                    metadata.topic(), metadata.partition(), metadata.offset());
        }
    }

    private static String bootstrapServers() {
        return "localhost:" + kafkaPort;
    }

    private static int findFreePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
