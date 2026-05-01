package eu.kafkaesque.core.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.DescribeTransactionsRequest;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;

import java.nio.ByteBuffer;

import static eu.kafkaesque.core.handler.ResponseSerializer.serialize;
import static java.util.stream.Collectors.toList;

/**
 * Handles Kafka transaction coordinator API responses.
 *
 * <p>Covers the APIs used by transactional producers:</p>
 * <ul>
 *   <li>{@link ApiKeys#INIT_PRODUCER_ID} – assigns a stable producer ID and epoch</li>
 *   <li>{@link ApiKeys#ADD_PARTITIONS_TO_TXN} – registers partition involvement (acknowledged, not stored)</li>
 *   <li>{@link ApiKeys#ADD_OFFSETS_TO_TXN} – registers consumer-group involvement (acknowledged, not stored)</li>
 *   <li>{@link ApiKeys#END_TXN} – commits or aborts the open transaction</li>
 *   <li>{@link ApiKeys#WRITE_TXN_MARKERS} – broker-internal marker writing (returns success; records are
 *       already updated by {@link #generateEndTxnResponse})</li>
 *   <li>{@link ApiKeys#TXN_OFFSET_COMMIT} – commits offsets transactionally (delegated to
 *       {@link GroupCoordinator})</li>
 * </ul>
 *
 * @see KafkaProtocolHandler
 * @see TransactionCoordinator
 */
@Slf4j
@RequiredArgsConstructor
final class TransactionApiHandler {

    private final TransactionCoordinator transactionCoordinator;
    private final GroupCoordinator groupCoordinator;

    /**
     * Generates an INIT_PRODUCER_ID response, assigning (or bumping) a producer ID and epoch.
     *
     * @param requestHeader the request header
     * @param request       the parsed INIT_PRODUCER_ID request
     * @return the serialised response buffer
     */
    ByteBuffer generateInitProducerIdResponse(final RequestHeader requestHeader, final InitProducerIdRequest request) {
        final var data = request.data();

        final var result = transactionCoordinator.initProducerId(
            data.transactionalId(), data.producerId(), data.producerEpoch());

        log.info("INIT_PRODUCER_ID: transactionalId={}, requestProducerId={}, requestEpoch={} → producerId={}, epoch={}",
            data.transactionalId(), data.producerId(), data.producerEpoch(),
            result.producerId(), result.epoch());

        final var response = new InitProducerIdResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode((short) 0)
            .setProducerId(result.producerId())
            .setProducerEpoch(result.epoch());

        return serialize(requestHeader, response, ApiKeys.INIT_PRODUCER_ID);
    }

    /**
     * Generates an ADD_PARTITIONS_TO_TXN response.
     *
     * <p>The mock acknowledges the request successfully without persisting partition
     * membership; the actual pending records are tracked by the {@link eu.kafkaesque.core.storage.EventStore}
     * when each {@code PRODUCE} request arrives.</p>
     *
     * <p>API versions 0–3 are sent by clients and use {@code v3AndBelowTopics} /
     * {@code resultsByTopicV3AndBelow}. Version 4+ are broker-to-broker messages
     * that use {@code transactions} / {@code resultsByTransaction}.</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed ADD_PARTITIONS_TO_TXN request
     * @return the serialised response buffer
     */
    ByteBuffer generateAddPartitionsToTxnResponse(final RequestHeader requestHeader, final AddPartitionsToTxnRequest request) {
        final var data = request.data();

        final var response = new AddPartitionsToTxnResponseData().setThrottleTimeMs(0);

        if (requestHeader.apiVersion() >= 4) {
            response.setResultsByTransaction(buildResultsByTransaction(data));
        } else {
            response.setResultsByTopicV3AndBelow(buildResultsByTopicV3AndBelow(data));
        }

        return serialize(requestHeader, response, ApiKeys.ADD_PARTITIONS_TO_TXN);
    }

    /**
     * Generates an ADD_OFFSETS_TO_TXN response (acknowledged with success).
     *
     * @param requestHeader the request header
     * @param request       the parsed ADD_OFFSETS_TO_TXN request
     * @return the serialised response buffer
     */
    ByteBuffer generateAddOffsetsToTxnResponse(final RequestHeader requestHeader, final AddOffsetsToTxnRequest request) {
        final var data = request.data();

        log.debug("ADD_OFFSETS_TO_TXN: transactionalId={}, groupId={}",
            data.transactionalId(), data.groupId());

        return serialize(requestHeader,
            new AddOffsetsToTxnResponseData().setThrottleTimeMs(0).setErrorCode((short) 0),
            ApiKeys.ADD_OFFSETS_TO_TXN);
    }

    /**
     * Generates an END_TXN response, committing or aborting the open transaction.
     *
     * <p>On commit, any buffered {@code TXN_OFFSET_COMMIT} entries are applied to the
     * group coordinator. On abort they are discarded, mirroring real Kafka behaviour
     * where transactional offset commits are only made visible on transaction commit.</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed END_TXN request
     * @return the serialised response buffer
     */
    ByteBuffer generateEndTxnResponse(final RequestHeader requestHeader, final EndTxnRequest request) {
        final var data = request.data();

        log.debug("END_TXN: transactionalId={}, committed={}",
            data.transactionalId(), data.committed());

        transactionCoordinator.endTransaction(data.transactionalId(), data.committed());

        final var pendingCommits =
            transactionCoordinator.drainPendingOffsetCommits(data.transactionalId());
        if (data.committed()) {
            pendingCommits.forEach(c ->
                groupCoordinator.commitOffset(c.groupId(), c.topic(), c.partitionIndex(), c.offset()));
            log.debug("Applied {} buffered TxnOffsetCommit(s) for transactionalId={}",
                pendingCommits.size(), data.transactionalId());
        } else {
            log.debug("Discarded {} buffered TxnOffsetCommit(s) for transactionalId={}",
                pendingCommits.size(), data.transactionalId());
        }

        return serialize(requestHeader,
            new EndTxnResponseData().setThrottleTimeMs(0).setErrorCode((short) 0),
            ApiKeys.END_TXN);
    }

    /**
     * Generates a WRITE_TXN_MARKERS response (broker-internal; returns success).
     *
     * <p>Transaction markers are already applied inline when {@code END_TXN} is processed.
     * This handler exists to satisfy the protocol when the broker sends the request to itself.</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed WRITE_TXN_MARKERS request
     * @return the serialised response buffer
     */
    ByteBuffer generateWriteTxnMarkersResponse(final RequestHeader requestHeader, final WriteTxnMarkersRequest request) {
        final var data = request.data();

        final var markerResults = data.markers().stream()
            .map(this::buildWriteTxnMarkerResult)
            .collect(toList());

        return serialize(requestHeader,
            new WriteTxnMarkersResponseData().setMarkers(markerResults),
            ApiKeys.WRITE_TXN_MARKERS);
    }

    /**
     * Generates a TXN_OFFSET_COMMIT response, buffering the offsets inside the transaction.
     *
     * <p>The offsets are not committed to the group coordinator immediately. They are held
     * as pending until the transaction ends: on commit they are applied; on abort they are
     * discarded. This matches real Kafka behaviour where transactional offset commits only
     * become visible once the owning transaction commits.</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed TXN_OFFSET_COMMIT request
     * @return the serialised response buffer
     */
    ByteBuffer generateTxnOffsetCommitResponse(final RequestHeader requestHeader, final TxnOffsetCommitRequest request) {
        final var data = request.data();

        final var topicResponses = data.topics().stream()
            .map(topic -> new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                .setName(topic.name())
                .setPartitions(topic.partitions().stream()
                    .map(partition -> {
                        transactionCoordinator.addPendingOffsetCommit(
                            data.transactionalId(), data.groupId(), topic.name(),
                            partition.partitionIndex(), partition.committedOffset());
                        return new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partition.partitionIndex())
                            .setErrorCode((short) 0);
                    })
                    .collect(toList())))
            .collect(toList());

        return serialize(requestHeader,
            new TxnOffsetCommitResponseData().setThrottleTimeMs(0).setTopics(topicResponses),
            ApiKeys.TXN_OFFSET_COMMIT);
    }

    /**
     * Builds a v4+ {@code resultsByTransaction} collection for an ADD_PARTITIONS_TO_TXN response.
     *
     * @param request the parsed ADD_PARTITIONS_TO_TXN request (v4+)
     * @return the collection of per-transaction results
     */
    private AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection buildResultsByTransaction(
            final AddPartitionsToTxnRequestData request) {
        final var results = new AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection();
        request.transactions().forEach(txn -> {
            final var topicResults = buildTopicResultCollection(txn.topics());
            results.add(new AddPartitionsToTxnResponseData.AddPartitionsToTxnResult()
                .setTransactionalId(txn.transactionalId())
                .setTopicResults(topicResults));
        });
        return results;
    }

    /**
     * Builds the v0–3 {@code resultsByTopicV3AndBelow} collection for an ADD_PARTITIONS_TO_TXN response.
     *
     * @param request the parsed ADD_PARTITIONS_TO_TXN request (v0–3)
     * @return the collection of per-topic results
     */
    private AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection buildResultsByTopicV3AndBelow(
            final AddPartitionsToTxnRequestData request) {
        return buildTopicResultCollection(request.v3AndBelowTopics());
    }

    /**
     * Builds a topic result collection from the given list of topics.
     *
     * @param topics the topics to build results for
     * @return the collection
     */
    private AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection buildTopicResultCollection(
            final Iterable<AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic> topics) {
        final var topicResults =
            new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection();
        topics.forEach(topic -> {
            final var partResults =
                new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResultCollection();
            topic.partitions().forEach(p ->
                partResults.add(new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult()
                    .setPartitionIndex(p)
                    .setPartitionErrorCode((short) 0)));
            topicResults.add(new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult()
                .setName(topic.name())
                .setResultsByPartition(partResults));
        });
        return topicResults;
    }

    /**
     * Builds a single marker result for a WRITE_TXN_MARKERS response.
     *
     * @param marker the write marker entry from the request
     * @return the corresponding result entry
     */
    private WriteTxnMarkersResponseData.WritableTxnMarkerResult buildWriteTxnMarkerResult(
            final WriteTxnMarkersRequestData.WritableTxnMarker marker) {
        final var topicResults = marker.topics().stream()
            .map(topic -> new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
                .setName(topic.name())
                .setPartitions(topic.partitionIndexes().stream()
                    .map(p -> new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                        .setPartitionIndex(p)
                        .setErrorCode((short) 0))
                    .collect(toList())))
            .collect(toList());
        return new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
            .setProducerId(marker.producerId())
            .setTopics(topicResults);
    }

    /**
     * Generates a LIST_TRANSACTIONS response listing all known transactional producers.
     *
     * <p>The {@code request} parameter is part of the dispatcher contract; the listing is
     * unfiltered. The optional state and producer-id filters carried by newer versions of the
     * request are not honoured by the mock — every known transactional producer is returned
     * with state "Ongoing".</p>
     *
     * @param requestHeader the request header
     * @param request       the parsed LIST_TRANSACTIONS request (currently unused)
     * @return the serialised response buffer
     */
    ByteBuffer generateListTransactionsResponse(
            final RequestHeader requestHeader, final ListTransactionsRequest request) {
        final var states = transactionCoordinator.getTransactionalIds().stream()
            .map(txnId -> new ListTransactionsResponseData.TransactionState()
                .setTransactionalId(txnId)
                .setProducerId(transactionCoordinator.getProducerIdAndEpoch(txnId)
                    .orElseThrow(() -> new IllegalStateException(
                        "Producer state missing for known transactional ID: " + txnId))
                    .producerId())
                .setTransactionState("Ongoing"))
            .collect(toList());

        final var response = new ListTransactionsResponseData()
            .setErrorCode((short) 0)
            .setTransactionStates(states);

        return serialize(requestHeader, response, ApiKeys.LIST_TRANSACTIONS);
    }

    /**
     * Generates a DESCRIBE_TRANSACTIONS response with details for the requested
     * transactional producers.
     *
     * @param requestHeader the request header
     * @param request       the parsed DESCRIBE_TRANSACTIONS request
     * @return the serialised response buffer
     */
    ByteBuffer generateDescribeTransactionsResponse(
            final RequestHeader requestHeader, final DescribeTransactionsRequest request) {
        final var data = request.data();

        final var states = data.transactionalIds().stream()
            .map(txnId -> transactionCoordinator.getProducerIdAndEpoch(txnId)
                .map(idAndEpoch -> ongoingTransactionState(txnId, idAndEpoch))
                .orElseGet(() -> unknownTransactionState(txnId)))
            .collect(toList());

        final var response = new DescribeTransactionsResponseData()
            .setTransactionStates(states);

        return serialize(
            requestHeader, response, ApiKeys.DESCRIBE_TRANSACTIONS);
    }

    /**
     * Builds the describe-transactions entry for an unknown transactional ID. The Kafka
     * protocol expects a successful response with sentinel values rather than an error.
     *
     * @param txnId the transactional producer ID that was not found
     * @return a transaction-state entry with empty/sentinel fields
     */
    private static DescribeTransactionsResponseData.TransactionState unknownTransactionState(final String txnId) {
        return new DescribeTransactionsResponseData.TransactionState()
            .setTransactionalId(txnId)
            .setErrorCode((short) 0)
            .setTransactionState("")
            .setProducerId(-1L)
            .setProducerEpoch((short) -1)
            .setTransactionTimeoutMs(0)
            .setTransactionStartTimeMs(-1L)
            .setTopics(new DescribeTransactionsResponseData.TopicDataCollection());
    }

    /**
     * Builds the describe-transactions entry for an ongoing transaction.
     *
     * @param txnId      the transactional producer ID
     * @param idAndEpoch the assigned producer ID and epoch
     * @return a transaction-state entry reporting the ongoing transaction
     */
    private static DescribeTransactionsResponseData.TransactionState ongoingTransactionState(
            final String txnId, final TransactionCoordinator.ProducerIdAndEpoch idAndEpoch) {
        return new DescribeTransactionsResponseData.TransactionState()
            .setTransactionalId(txnId)
            .setErrorCode((short) 0)
            .setTransactionState("Ongoing")
            .setProducerId(idAndEpoch.producerId())
            .setProducerEpoch(idAndEpoch.epoch())
            .setTransactionTimeoutMs(60000)
            .setTransactionStartTimeMs(System.currentTimeMillis())
            .setTopics(new DescribeTransactionsResponseData.TopicDataCollection());
    }
}
