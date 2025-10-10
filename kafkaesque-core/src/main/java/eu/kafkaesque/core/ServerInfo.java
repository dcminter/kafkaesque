package eu.kafkaesque.core;

import java.io.IOException;

/**
 * Interface for accessing server information.
 * This abstraction allows for easier testing of protocol handlers.
 */
public interface ServerInfo {

    /**
     * Get the port the server is listening on.
     *
     * @return the server port
     * @throws IOException if the port cannot be determined
     */
    int getPort() throws IOException;

    /**
     * Get the host the server is listening on.
     *
     * @return the server host
     */
    String getHost();
}
