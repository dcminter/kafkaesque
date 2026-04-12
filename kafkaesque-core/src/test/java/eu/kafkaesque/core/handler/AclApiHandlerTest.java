package eu.kafkaesque.core.handler;

import eu.kafkaesque.core.storage.AclStore;
import eu.kafkaesque.core.storage.AclStore.AclBinding;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;

import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AclApiHandler}.
 */
@Slf4j
class AclApiHandlerTest {

    /** Kafka resource type code for TOPIC. */
    private static final byte TOPIC_TYPE = 2;

    /** Kafka pattern type code for LITERAL. */
    private static final byte LITERAL_TYPE = 3;

    /** Kafka ACL operation code for READ. */
    private static final byte READ_OP = 3;

    /** Kafka ACL operation code for WRITE. */
    private static final byte WRITE_OP = 4;

    /** Kafka permission type code for ALLOW. */
    private static final byte ALLOW_PERM = 3;

    /** Kafka wire-protocol code for the ANY enum value (wildcard in filters). */
    private static final byte ANY_CODE = 1;

    private AclStore aclStore;
    private AclApiHandler handler;

    @BeforeEach
    void setUp() {
        aclStore = new AclStore();
        handler = new AclApiHandler(aclStore);
    }

    @Test
    void shouldStoreBindingsAndReturnZeroErrorCodes() {
        // When
        final var response = invokeCreateAcls(
            creation("my-topic", "User:alice", READ_OP),
            creation("my-topic", "User:bob", WRITE_OP));

        // Then — bindings are stored
        assertThat(aclStore.getBindings()).hasSize(2);

        // And — response has zero-error results
        assertThat(response).isNotNull();
        final var responseData = parseCreateAclsResponse(response);
        assertThat(responseData.results()).hasSize(2);
        assertThat(responseData.results()).allSatisfy(
            r -> assertThat(r.errorCode()).isEqualTo((short) 0));
    }

    @Test
    void shouldReturnNullOnMalformedCreateAclsRequest() {
        // Given
        final var header = new RequestHeader(
            ApiKeys.CREATE_ACLS, ApiKeys.CREATE_ACLS.latestVersion(), "test-client", 1);
        final var emptyBuffer = ByteBuffer.allocate(0);

        // When
        log.info("Expecting an error log from AclApiHandler due to malformed (empty) request buffer");
        final var response = handler.generateCreateAclsResponse(header, emptyBuffer);

        // Then
        assertThat(response).isNull();
    }

    @Test
    void shouldDescribeMatchingBindings() {
        // Given — pre-populate store
        aclStore.addBinding(new AclBinding(
            TOPIC_TYPE, "my-topic", LITERAL_TYPE, "User:alice", "*", READ_OP, ALLOW_PERM));

        // When — describe with ANY filter (all wildcards)
        final var response = invokeDescribeAcls(
            ANY_CODE, null, ANY_CODE, null, null, ANY_CODE, ANY_CODE);

        // Then
        assertThat(response).isNotNull();
        final var responseData = parseDescribeAclsResponse(response);
        assertThat(responseData.errorCode()).isEqualTo((short) 0);
        assertThat(responseData.resources()).hasSize(1);
        assertThat(responseData.resources().get(0).resourceName()).isEqualTo("my-topic");
        assertThat(responseData.resources().get(0).acls()).hasSize(1);
    }

    @Test
    void shouldGroupBindingsByResourceKey() {
        // Given — two bindings for same topic, different principals
        aclStore.addBinding(new AclBinding(
            TOPIC_TYPE, "my-topic", LITERAL_TYPE, "User:alice", "*", READ_OP, ALLOW_PERM));
        aclStore.addBinding(new AclBinding(
            TOPIC_TYPE, "my-topic", LITERAL_TYPE, "User:bob", "*", READ_OP, ALLOW_PERM));

        // When
        final var response = invokeDescribeAcls(
            ANY_CODE, null, ANY_CODE, null, null, ANY_CODE, ANY_CODE);

        // Then — one resource with 2 ACL descriptions
        final var responseData = parseDescribeAclsResponse(response);
        assertThat(responseData.resources()).hasSize(1);
        final var resource = responseData.resources().get(0);
        assertThat(resource.resourceName()).isEqualTo("my-topic");
        assertThat(resource.acls()).hasSize(2);

        final var principals = resource.acls().stream()
            .map(DescribeAclsResponseData.AclDescription::principal)
            .collect(Collectors.toList());
        assertThat(principals).containsExactlyInAnyOrder("User:alice", "User:bob");
    }

    @Test
    void shouldReturnEmptyResourcesWhenNoBindingsMatch() {
        // When — describe on empty store
        final var response = invokeDescribeAcls(
            ANY_CODE, null, ANY_CODE, null, null, ANY_CODE, ANY_CODE);

        // Then
        final var responseData = parseDescribeAclsResponse(response);
        assertThat(responseData.resources()).isEmpty();
    }

    @Test
    void shouldDeleteMatchingBindingsAndReturnThem() {
        // Given
        aclStore.addBinding(new AclBinding(
            TOPIC_TYPE, "my-topic", LITERAL_TYPE, "User:alice", "*", READ_OP, ALLOW_PERM));

        // When
        final var response = invokeDeleteAcls(
            TOPIC_TYPE, "my-topic", LITERAL_TYPE, "User:alice", "*", READ_OP, ALLOW_PERM);

        // Then — store is empty
        assertThat(aclStore.getBindings()).isEmpty();

        // And — response lists the deleted binding
        final var responseData = parseDeleteAclsResponse(response);
        assertThat(responseData.filterResults()).hasSize(1);
        final var filterResult = responseData.filterResults().get(0);
        assertThat(filterResult.errorCode()).isEqualTo((short) 0);
        assertThat(filterResult.matchingAcls()).hasSize(1);
        assertThat(filterResult.matchingAcls().get(0).resourceName()).isEqualTo("my-topic");
    }

    @Test
    void shouldReturnEmptyMatchingAclsWhenNothingMatches() {
        // When — delete on empty store
        final var response = invokeDeleteAcls(
            TOPIC_TYPE, "no-such-topic", LITERAL_TYPE, "User:alice", "*", READ_OP, ALLOW_PERM);

        // Then
        final var responseData = parseDeleteAclsResponse(response);
        assertThat(responseData.filterResults()).hasSize(1);
        assertThat(responseData.filterResults().get(0).matchingAcls()).isEmpty();
    }

    // --- helpers ---

    /**
     * Creates a {@link CreateAclsRequestData.AclCreation} for a TOPIC/LITERAL/ALLOW binding.
     *
     * @param topicName the topic name
     * @param principal the principal
     * @param operation the operation code
     * @return the creation entry
     */
    private static CreateAclsRequestData.AclCreation creation(
            final String topicName, final String principal, final byte operation) {
        return new CreateAclsRequestData.AclCreation()
            .setResourceType(TOPIC_TYPE)
            .setResourceName(topicName)
            .setResourcePatternType(LITERAL_TYPE)
            .setPrincipal(principal)
            .setHost("*")
            .setOperation(operation)
            .setPermissionType(ALLOW_PERM);
    }

    private ByteBuffer invokeCreateAcls(final CreateAclsRequestData.AclCreation... creations) {
        final short apiVersion = ApiKeys.CREATE_ACLS.latestVersion();
        final var requestData = new CreateAclsRequestData().setCreations(of(creations));
        final var buffer = serialize(requestData, apiVersion);
        final var header = new RequestHeader(ApiKeys.CREATE_ACLS, apiVersion, "test-client", 1);
        return handler.generateCreateAclsResponse(header, buffer);
    }

    private ByteBuffer invokeDescribeAcls(
            final byte resourceType, final String resourceName,
            final byte patternType, final String principal,
            final String host, final byte operation, final byte permissionType) {
        final short apiVersion = ApiKeys.DESCRIBE_ACLS.latestVersion();
        final var requestData = new DescribeAclsRequestData()
            .setResourceTypeFilter(resourceType)
            .setResourceNameFilter(resourceName)
            .setPatternTypeFilter(patternType)
            .setPrincipalFilter(principal)
            .setHostFilter(host)
            .setOperation(operation)
            .setPermissionType(permissionType);
        final var buffer = serialize(requestData, apiVersion);
        final var header = new RequestHeader(ApiKeys.DESCRIBE_ACLS, apiVersion, "test-client", 1);
        return handler.generateDescribeAclsResponse(header, buffer);
    }

    private ByteBuffer invokeDeleteAcls(
            final byte resourceType, final String resourceName,
            final byte patternType, final String principal,
            final String host, final byte operation, final byte permissionType) {
        final short apiVersion = ApiKeys.DELETE_ACLS.latestVersion();
        final var filter = new DeleteAclsRequestData.DeleteAclsFilter()
            .setResourceTypeFilter(resourceType)
            .setResourceNameFilter(resourceName)
            .setPatternTypeFilter(patternType)
            .setPrincipalFilter(principal)
            .setHostFilter(host)
            .setOperation(operation)
            .setPermissionType(permissionType);
        final var requestData = new DeleteAclsRequestData().setFilters(of(filter));
        final var buffer = serialize(requestData, apiVersion);
        final var header = new RequestHeader(ApiKeys.DELETE_ACLS, apiVersion, "test-client", 1);
        return handler.generateDeleteAclsResponse(header, buffer);
    }

    private static ByteBuffer serialize(
            final org.apache.kafka.common.protocol.ApiMessage message, final short apiVersion) {
        final var cache = new ObjectSerializationCache();
        final int bodySize = message.size(cache, apiVersion);
        final var buffer = ByteBuffer.allocate(bodySize);
        message.write(new ByteBufferAccessor(buffer), cache, apiVersion);
        buffer.flip();
        return buffer;
    }

    private static CreateAclsResponseData parseCreateAclsResponse(final ByteBuffer buffer) {
        final short apiVersion = ApiKeys.CREATE_ACLS.latestVersion();
        skipResponseHeader(buffer, ApiKeys.CREATE_ACLS, apiVersion);
        final var responseData = new CreateAclsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private static DescribeAclsResponseData parseDescribeAclsResponse(final ByteBuffer buffer) {
        final short apiVersion = ApiKeys.DESCRIBE_ACLS.latestVersion();
        skipResponseHeader(buffer, ApiKeys.DESCRIBE_ACLS, apiVersion);
        final var responseData = new DescribeAclsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    private static DeleteAclsResponseData parseDeleteAclsResponse(final ByteBuffer buffer) {
        final short apiVersion = ApiKeys.DELETE_ACLS.latestVersion();
        skipResponseHeader(buffer, ApiKeys.DELETE_ACLS, apiVersion);
        final var responseData = new DeleteAclsResponseData();
        responseData.read(new ByteBufferAccessor(buffer), apiVersion);
        return responseData;
    }

    /**
     * Skips the response header bytes in a serialised response buffer.
     *
     * @param buffer     the response buffer
     * @param apiKey     the API key
     * @param apiVersion the API version
     */
    private static void skipResponseHeader(
            final ByteBuffer buffer, final ApiKeys apiKey, final short apiVersion) {
        final short headerVersion = apiKey.responseHeaderVersion(apiVersion);
        final int headerBytes = (headerVersion >= 1) ? 5 : 4;
        buffer.position(buffer.position() + headerBytes);
    }
}
