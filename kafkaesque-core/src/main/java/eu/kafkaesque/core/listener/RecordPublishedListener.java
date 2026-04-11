package eu.kafkaesque.core.listener;

import eu.kafkaesque.core.storage.StoredRecord;

/**
 * Listener that receives a callback when a record is published to the Kafkaesque server.
 *
 * <p>The listener is invoked for every record stored, including records that are part of
 * a pending transaction. Implementations should complete quickly to avoid blocking the
 * server's NIO event-loop thread.</p>
 *
 * @see ListenerRegistry
 * @see StoredRecord
 */
@FunctionalInterface
public interface RecordPublishedListener {

    /**
     * Called when a record has been published and stored on the server.
     *
     * @param record the stored record, including its assigned offset and all metadata
     */
    void onRecordPublished(StoredRecord record);
}
