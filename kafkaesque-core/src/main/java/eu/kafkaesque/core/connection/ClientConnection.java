package eu.kafkaesque.core.connection;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Represents a client connection to the Kafkaesque server.
 * Manages reading and writing data to/from the client using NIO channels.
 *
 * <p>This class encapsulates the socket channel and associated buffers for a single
 * client connection. While the channel and buffers themselves are immutable references,
 * the buffer contents are mutable as they are used for I/O operations.</p>
 *
 * <p>Note: This class uses Lombok's {@code @Getter} with {@code @Accessors(fluent = true)}
 * to generate record-style accessor methods (e.g., {@code channel()} instead of {@code getChannel()}).</p>
 */
@Getter
@Accessors(fluent = true)
public final class ClientConnection {

    /**
     * The NIO socket channel for this client connection.
     */
    private final SocketChannel channel;

    /**
     * Buffer for reading data from the client.
     * Capacity: 8192 bytes.
     */
    private final ByteBuffer readBuffer;

    /**
     * Buffer for writing data to the client.
     * Capacity: 8192 bytes.
     */
    private final ByteBuffer writeBuffer;

    /**
     * Creates a new client connection wrapper for the given socket channel.
     *
     * @param channel the socket channel for this connection
     */
    public ClientConnection(final SocketChannel channel) {
        this.channel = channel;
        this.readBuffer = ByteBuffer.allocate(8192);
        this.writeBuffer = ByteBuffer.allocate(8192);
    }

    /**
     * Reads data from the socket channel into the read buffer.
     *
     * @return the number of bytes read, or -1 if the channel has reached end-of-stream
     * @throws IOException if an I/O error occurs
     */
    public int read() throws IOException {
        return channel.read(readBuffer);
    }

    /**
     * Writes data from the write buffer to the socket channel.
     *
     * @return the number of bytes written
     * @throws IOException if an I/O error occurs
     */
    public int write() throws IOException {
        return channel.write(writeBuffer);
    }
}
