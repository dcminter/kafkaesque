package eu.kafkaesque.core;

import org.apache.kafka.common.compress.Compression;

/**
 * Compression codecs supported by Kafkaesque for topic FETCH responses.
 *
 * <p>This is Kafkaesque's own compression enum, decoupled from the Kafka client library.
 * It allows users to specify compression when creating topics without depending on a
 * specific version of the Kafka client.</p>
 */
public enum CompressionCodec {

    /** No compression. */
    NONE,

    /** GZIP compression. */
    GZIP,

    /** Snappy compression. */
    SNAPPY,

    /** LZ4 compression. */
    LZ4,

    /** Zstandard compression. */
    ZSTD;

    /**
     * Converts this codec to the Kafka {@link Compression} type.
     *
     * <p>Package-private: only used internally to bridge to the Kafka wire protocol.</p>
     *
     * @return the corresponding Kafka compression
     */
    Compression toKafkaCompression() {
        switch (this) {
            case GZIP:
                return Compression.gzip().build();
            case SNAPPY:
                return Compression.snappy().build();
            case LZ4:
                return Compression.lz4().build();
            case ZSTD:
                return Compression.zstd().build();
            default:
                return Compression.NONE;
        }
    }
}
