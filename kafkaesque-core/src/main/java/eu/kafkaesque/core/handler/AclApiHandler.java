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
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/**
 * Handles Kafka ACL API responses for {@link ApiKeys#CREATE_ACLS},
 * {@link ApiKeys#DESCRIBE_ACLS}, and {@link ApiKeys#DELETE_ACLS}.
 *
 * <p>Parses incoming wire-protocol requests, delegates storage operations to the
 * {@link AclStore}, and builds the corresponding response messages. No access
 * control enforcement is performed; the handler only manages ACL binding CRUD.</p>
 *
 * @see AclStore
 * @see KafkaProtocolHandler
 */
@Slf4j
final class AclApiHandler {

    /** The backing ACL store. */
    private final AclStore aclStore;

    /**
     * Creates a new handler backed by the given ACL store.
     *
     * @param aclStore the store that receives and serves ACL bindings
     */
    AclApiHandler(final AclStore aclStore) {
        this.aclStore = aclStore;
    }

    /**
     * Generates a {@link ApiKeys#CREATE_ACLS} response after storing the requested bindings.
     *
     * <p>Each creation in the request is added to the store and a success result
     * (error code 0) is returned.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or {@code null} on error
     */
    ByteBuffer generateCreateAclsResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new CreateAclsRequestData(accessor, requestHeader.apiVersion());

            final var results = request.creations().stream()
                .map(this::createAndBuildResult)
                .toList();

            final var response = new CreateAclsResponseData()
                .setThrottleTimeMs(0)
                .setResults(results);

            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.CREATE_ACLS);
        } catch (final Exception e) {
            log.error("Error generating CreateAcls response", e);
            return null;
        }
    }

    /**
     * Generates a {@link ApiKeys#DESCRIBE_ACLS} response for the requested filters.
     *
     * <p>Matching bindings are grouped by (resourceType, resourceName, patternType)
     * into {@link DescribeAclsResponseData.DescribeAclsResource} entries.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or {@code null} on error
     */
    ByteBuffer generateDescribeAclsResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new DescribeAclsRequestData(accessor, requestHeader.apiVersion());

            final var matched = aclStore.findMatchingBindings(
                request.resourceTypeFilter(), request.resourceNameFilter(),
                request.patternTypeFilter(), request.principalFilter(),
                request.hostFilter(), request.operation(), request.permissionType());

            final var resources = groupBindingsIntoResources(matched);

            final var response = new DescribeAclsResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode((short) 0)
                .setErrorMessage(null)
                .setResources(resources);

            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.DESCRIBE_ACLS);
        } catch (final Exception e) {
            log.error("Error generating DescribeAcls response", e);
            return null;
        }
    }

    /**
     * Generates a {@link ApiKeys#DELETE_ACLS} response after removing matching bindings.
     *
     * <p>Each filter in the request triggers a deletion of matching bindings from the
     * store. A {@link DeleteAclsResponseData.DeleteAclsFilterResult} is returned for
     * each filter, listing the bindings that were removed.</p>
     *
     * @param requestHeader the request header
     * @param buffer        the buffer containing the request body
     * @return the serialised response buffer, or {@code null} on error
     */
    ByteBuffer generateDeleteAclsResponse(final RequestHeader requestHeader, final ByteBuffer buffer) {
        try {
            final var accessor = new ByteBufferAccessor(buffer);
            final var request = new DeleteAclsRequestData(accessor, requestHeader.apiVersion());

            final var filterResults = request.filters().stream()
                .map(this::deleteAndBuildFilterResult)
                .toList();

            final var response = new DeleteAclsResponseData()
                .setThrottleTimeMs(0)
                .setFilterResults(filterResults);

            return ResponseSerializer.serialize(requestHeader, response, ApiKeys.DELETE_ACLS);
        } catch (final Exception e) {
            log.error("Error generating DeleteAcls response", e);
            return null;
        }
    }

    /**
     * Stores a single ACL creation and builds the corresponding result entry.
     *
     * @param creation the ACL creation from the request
     * @return the result entry with error code 0
     */
    private CreateAclsResponseData.AclCreationResult createAndBuildResult(
            final CreateAclsRequestData.AclCreation creation) {
        final var binding = new AclBinding(
            creation.resourceType(), creation.resourceName(),
            creation.resourcePatternType(), creation.principal(),
            creation.host(), creation.operation(), creation.permissionType());
        aclStore.addBinding(binding);
        return new CreateAclsResponseData.AclCreationResult()
            .setErrorCode((short) 0)
            .setErrorMessage(null);
    }

    /**
     * Groups a list of ACL bindings into describe-response resource entries.
     *
     * <p>Bindings are grouped by (resourceType, resourceName, patternType) and each
     * group becomes a {@link DescribeAclsResponseData.DescribeAclsResource} with its
     * ACL descriptions.</p>
     *
     * @param bindings the bindings to group
     * @return the grouped resource entries
     */
    private static List<DescribeAclsResponseData.DescribeAclsResource> groupBindingsIntoResources(
            final List<AclBinding> bindings) {
        final Map<String, List<AclBinding>> grouped = bindings.stream()
            .collect(groupingBy(b -> b.resourceType() + ":" + b.resourceName() + ":" + b.resourcePatternType()));

        return grouped.values().stream()
            .map(AclApiHandler::buildResourceEntry)
            .toList();
    }

    /**
     * Builds a single describe-response resource entry from a group of bindings
     * that share the same resource key.
     *
     * @param group the bindings sharing the same (resourceType, resourceName, patternType)
     * @return the resource entry
     */
    private static DescribeAclsResponseData.DescribeAclsResource buildResourceEntry(final List<AclBinding> group) {
        final var first = group.getFirst();
        final var acls = group.stream()
            .map(b -> new DescribeAclsResponseData.AclDescription()
                .setPrincipal(b.principal())
                .setHost(b.host())
                .setOperation(b.operation())
                .setPermissionType(b.permissionType()))
            .collect(Collectors.toList());

        return new DescribeAclsResponseData.DescribeAclsResource()
            .setResourceType(first.resourceType())
            .setResourceName(first.resourceName())
            .setPatternType(first.resourcePatternType())
            .setAcls(acls);
    }

    /**
     * Deletes bindings matching a single filter and builds the corresponding result.
     *
     * @param filter the delete filter from the request
     * @return the filter result listing all removed bindings
     */
    private DeleteAclsResponseData.DeleteAclsFilterResult deleteAndBuildFilterResult(
            final DeleteAclsRequestData.DeleteAclsFilter filter) {
        final var deleted = aclStore.deleteMatchingBindings(
            filter.resourceTypeFilter(), filter.resourceNameFilter(),
            filter.patternTypeFilter(), filter.principalFilter(),
            filter.hostFilter(), filter.operation(), filter.permissionType());

        final var matchingAcls = deleted.stream()
            .map(AclApiHandler::buildMatchingAcl)
            .toList();

        return new DeleteAclsResponseData.DeleteAclsFilterResult()
            .setErrorCode((short) 0)
            .setErrorMessage(null)
            .setMatchingAcls(matchingAcls);
    }

    /**
     * Builds a delete-response matching-ACL entry from a removed binding.
     *
     * @param binding the removed binding
     * @return the matching-ACL entry
     */
    private static DeleteAclsResponseData.DeleteAclsMatchingAcl buildMatchingAcl(final AclBinding binding) {
        return new DeleteAclsResponseData.DeleteAclsMatchingAcl()
            .setErrorCode((short) 0)
            .setErrorMessage(null)
            .setResourceType(binding.resourceType())
            .setResourceName(binding.resourceName())
            .setPatternType(binding.resourcePatternType())
            .setPrincipal(binding.principal())
            .setHost(binding.host())
            .setOperation(binding.operation())
            .setPermissionType(binding.permissionType());
    }
}
