package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.copyOf;

/**
 * A TCP pass-through proxy intended for use in integration tests that verify idempotent
 * producer behaviour.
 *
 * <p>The proxy sits between a Kafka client and a real (or mock) Kafka broker. It forwards
 * all traffic transparently with two exceptions:</p>
 * <ol>
 *   <li><strong>Metadata response rewriting:</strong> {@code METADATA} responses are scanned
 *       for the broker's port encoded as a 4-byte big-endian integer; every occurrence is
 *       replaced with this proxy's own port so that all subsequent connections from the client
 *       (for {@code PRODUCE}, {@code INIT_PRODUCER_ID}, etc.) also flow through the proxy.
 *       Because the port occupies a fixed 4-byte field in the Kafka wire format, this
 *       replacement is both version-agnostic and size-preserving.</li>
 *   <li><strong>Selective PRODUCE response dropping:</strong> when
 *       {@link #armDropNextProduceResponse()} is called before a send, the broker's response
 *       to the next {@code PRODUCE} request is silently discarded. The client treats this as a
 *       timeout and retries the same record batch with the <em>same</em> producer ID, epoch,
 *       and sequence number. An idempotent-aware broker must deduplicate the retry; a broker
 *       that lacks server-side idempotency support will store the record twice.</li>
 * </ol>
 *
 * <p>For correct operation the Kafka producer pointed at this proxy must be configured
 * with {@code max.in.flight.requests.per.connection=1}, which makes the
 * request/response protocol strictly sequential on each connection and ensures the
 * correct request is attributed to each response.</p>
 *
 * <p>This class is not thread-safe for external callers; it is intended to be owned by a
 * single test method.</p>
 */
@Slf4j
final class DropAckProxy implements Closeable {

    /** Kafka wire-protocol API key for {@code PRODUCE} requests. */
    private static final short PRODUCE_API_KEY = 0;

    /** Kafka wire-protocol API key for {@code METADATA} requests. */
    private static final short METADATA_API_KEY = 3;

    /** Host of the real broker to which this proxy forwards traffic. */
    private final String targetHost;

    /** Port of the real broker to which this proxy forwards traffic. */
    private final int targetPort;

    /** 4-byte big-endian encoding of {@link #targetPort}, cached for fast scanning. */
    private final byte[] targetPortBytes;

    /** Server socket accepting incoming client connections. */
    private final ServerSocket proxySocket;

    /** Executor running the accept loop and per-connection handler threads. */
    private final ExecutorService executor;

    /**
     * One-shot flag; when {@code true} the proxy drops the next {@code PRODUCE} response.
     * Uses {@link AtomicBoolean#compareAndSet} to guarantee it fires exactly once across
     * multiple concurrent connections.
     */
    private final AtomicBoolean dropNextProduce;

    /**
     * Creates a proxy that forwards all connections to the broker at the given address.
     *
     * <p>Only the first {@code host:port} pair in a comma-separated list is used (matching
     * single-broker test setups).</p>
     *
     * <p>The proxy begins accepting connections immediately on a randomly assigned local
     * port. Use {@link #getBootstrapServers()} to obtain the address to pass to the
     * Kafka client.</p>
     *
     * @param bootstrapServers the broker address in {@code "host:port"} format
     * @throws IOException if the proxy server socket cannot be bound
     */
    DropAckProxy(final String bootstrapServers) throws IOException {
        final var rawPair = bootstrapServers.split(",")[0].trim();
        final var firstPair = rawPair.contains("://") ? rawPair.substring(rawPair.indexOf("://") + 3) : rawPair;
        final var colonPos = firstPair.lastIndexOf(':');
        this.targetHost = firstPair.substring(0, colonPos);
        this.targetPort = Integer.parseInt(firstPair.substring(colonPos + 1));
        this.targetPortBytes = ByteBuffer.allocate(Integer.BYTES).putInt(targetPort).array();
        this.proxySocket = new ServerSocket(0);
        this.executor = Executors.newCachedThreadPool();
        this.dropNextProduce = new AtomicBoolean(false);
        executor.submit(this::acceptConnections);
        log.debug("DropAckProxy listening on port {} → {}:{}", proxySocket.getLocalPort(), targetHost, targetPort);
    }

    /**
     * Returns the host of this proxy (always {@code "localhost"}).
     *
     * @return {@code "localhost"}
     */
    String getHost() {
        return "localhost";
    }

    /**
     * Returns the local port on which this proxy is listening.
     *
     * @return the bound local port
     */
    int getPort() {
        return proxySocket.getLocalPort();
    }

    /**
     * Returns a bootstrap servers string in {@code "host:port"} format for connecting
     * to this proxy.
     *
     * @return the proxy address as a bootstrap servers string
     */
    String getBootstrapServers() {
        return getHost() + ":" + getPort();
    }

    /**
     * Arms the proxy to drop the response to the next {@code PRODUCE} request.
     *
     * <p>After the response is dropped the arm is automatically cleared (one-shot).
     * The Kafka producer will time out waiting for the acknowledgement (after
     * {@code request.timeout.ms}) and retry the same batch with the same producer ID,
     * epoch, and sequence number.</p>
     */
    void armDropNextProduceResponse() {
        dropNextProduce.set(true);
        log.debug("DropAckProxy armed: next PRODUCE response will be dropped");
    }

    /**
     * Runs the accept loop: accepts client connections and spawns a handler thread for each.
     */
    private void acceptConnections() {
        while (!proxySocket.isClosed()) {
            try {
                final var clientSocket = proxySocket.accept();
                final var serverSock = new Socket(targetHost, targetPort);
                executor.submit(() -> handleConnection(clientSocket, serverSock));
            } catch (final IOException e) {
                if (!proxySocket.isClosed()) {
                    log.warn("Error accepting connection: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * Handles one client/server connection pair.
     *
     * <p>Reads length-prefixed request messages from the client, forwards them to the
     * broker, reads the broker's length-prefixed responses, and then either drops or
     * forwards each response according to the current proxy state. Metadata responses
     * are rewritten before forwarding so that broker addresses point back to this proxy.</p>
     *
     * @param clientSocket the socket connected to the Kafka client
     * @param serverSocket the socket connected to the real Kafka broker
     */
    private void handleConnection(final Socket clientSocket, final Socket serverSocket) {
        try (clientSocket; serverSocket;
             var clientIn = clientSocket.getInputStream();
             var clientOut = clientSocket.getOutputStream();
             var serverIn = serverSocket.getInputStream();
             var serverOut = serverSocket.getOutputStream()) {

            final var lenBuf = new byte[Integer.BYTES];

            while (true) {
                // Read the 4-byte length prefix of the request
                if (!readFully(clientIn, lenBuf)) {
                    break;
                }
                final int requestLen = ByteBuffer.wrap(lenBuf).getInt();
                final var requestBody = new byte[requestLen];
                if (!readFully(clientIn, requestBody)) {
                    break;
                }

                // Extract apiKey (bytes 0-1) from the request body
                final short apiKey = ByteBuffer.wrap(requestBody).getShort();

                // Forward the full request to the broker
                serverOut.write(lenBuf);
                serverOut.write(requestBody);
                serverOut.flush();

                // Read the broker's 4-byte length prefix response
                if (!readFully(serverIn, lenBuf)) {
                    break;
                }
                final int responseLen = ByteBuffer.wrap(lenBuf).getInt();
                var responseBody = new byte[responseLen];
                if (!readFully(serverIn, responseBody)) {
                    break;
                }

                // Rewrite broker port in METADATA responses so all subsequent
                // connections from the client continue to flow through this proxy.
                // The Kafka broker port is encoded as a fixed 4-byte big-endian int,
                // so replacing it preserves the message length and requires no
                // re-serialisation of the protocol framing.
                if (apiKey == METADATA_API_KEY) {
                    responseBody = rewriteBrokerPort(responseBody);
                }

                // Drop or forward
                if (apiKey == PRODUCE_API_KEY && dropNextProduce.compareAndSet(true, false)) {
                    log.debug("Dropped PRODUCE response to force client retry");
                } else {
                    clientOut.write(lenBuf);
                    clientOut.write(responseBody);
                    clientOut.flush();
                }
            }

        } catch (final IOException e) {
            log.debug("Connection handler exiting: {}", e.getMessage());
        }
    }

    /**
     * Rewrites all occurrences of the broker's port (as a 4-byte big-endian integer) in the
     * {@code METADATA} response body with this proxy's own port.
     *
     * <p>Broker ports are always encoded as fixed-width {@code int32} values in every version
     * of the Kafka metadata response. Because the replacement is byte-for-byte the same
     * width, the message length is unchanged and no protocol framing needs updating.</p>
     *
     * <p>Port values used in realistic test environments (ephemeral range 32768–60999) are
     * unlikely to collide with other 4-byte fields in the response (e.g. throttle time, node
     * IDs, error codes), making a plain byte scan safe in practice.</p>
     *
     * @param responseBody the response bytes (without the 4-byte wire length prefix)
     * @return a copy of {@code responseBody} with all occurrences of the broker port replaced
     */
    private byte[] rewriteBrokerPort(final byte[] responseBody) {
        final var proxyPortBytes = ByteBuffer.allocate(Integer.BYTES).putInt(getPort()).array();
        final var result = copyOf(responseBody, responseBody.length);
        var replaced = false;
        for (int i = 0; i <= result.length - Integer.BYTES; i++) {
            if (result[i] == targetPortBytes[0]
                    && result[i + 1] == targetPortBytes[1]
                    && result[i + 2] == targetPortBytes[2]
                    && result[i + 3] == targetPortBytes[3]) {
                System.arraycopy(proxyPortBytes, 0, result, i, Integer.BYTES);
                log.debug("Rewrote broker port {} → {} at offset {} in {}-byte response",
                    targetPort, getPort(), i, responseBody.length);
                replaced = true;
                i += Integer.BYTES - 1; // skip past the replaced bytes
            }
        }
        if (!replaced) {
            log.warn("Broker port {} not found in {}-byte METADATA response; broker address not rewritten",
                targetPort, responseBody.length);
        }
        return result;
    }

    /**
     * Reads exactly {@code buf.length} bytes from {@code in} into {@code buf}.
     *
     * @param in  the input stream to read from
     * @param buf the buffer to fill
     * @return {@code true} if the buffer was fully populated; {@code false} on clean EOF
     * @throws IOException if an I/O error occurs mid-stream
     */
    private static boolean readFully(final InputStream in, final byte[] buf) throws IOException {
        int offset = 0;
        while (offset < buf.length) {
            final int n = in.read(buf, offset, buf.length - offset);
            if (n == -1) {
                return false;
            }
            offset += n;
        }
        return true;
    }

    /**
     * Stops the proxy by closing the server socket and shutting down all connection threads.
     */
    @Override
    public void close() {
        try {
            proxySocket.close();
        } catch (final IOException e) {
            log.warn("Error closing proxy server socket: {}", e.getMessage());
        }
        executor.shutdownNow();
    }
}
