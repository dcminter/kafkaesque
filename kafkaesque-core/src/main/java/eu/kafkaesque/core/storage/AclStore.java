package eu.kafkaesque.core.storage;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Thread-safe CRUD store for ACL bindings.
 *
 * <p>This store only handles storage and retrieval of ACL bindings; it does not
 * enforce any access control. Kafkaesque uses it to satisfy the
 * {@code CREATE_ACLS}, {@code DESCRIBE_ACLS}, and {@code DELETE_ACLS} wire
 * protocol operations so that applications using the Kafka {@code AdminClient}
 * ACL API can be tested without a real broker.</p>
 *
 * @see AclBinding
 */
@Slf4j
public final class AclStore {

    /**
     * An ACL binding combining a resource pattern with an access control entry.
     */
    @EqualsAndHashCode
    @ToString
    public static final class AclBinding {

        /** The Kafka resource type code (e.g. TOPIC = 2). */
        private final byte resourceType;

        /** The name of the resource. */
        private final String resourceName;

        /** The pattern type code (e.g. LITERAL = 3). */
        private final byte resourcePatternType;

        /** The principal (e.g. {@code "User:test"}). */
        private final String principal;

        /** The host filter (e.g. {@code "*"}). */
        private final String host;

        /** The ACL operation code (e.g. READ = 3). */
        private final byte operation;

        /** The permission type code (e.g. ALLOW = 3). */
        private final byte permissionType;

        /**
         * Creates a new ACL binding.
         *
         * @param resourceType        the Kafka resource type code (e.g. TOPIC = 2)
         * @param resourceName        the name of the resource
         * @param resourcePatternType the pattern type code (e.g. LITERAL = 3)
         * @param principal           the principal (e.g. {@code "User:test"})
         * @param host                the host filter (e.g. {@code "*"})
         * @param operation           the ACL operation code (e.g. READ = 3)
         * @param permissionType      the permission type code (e.g. ALLOW = 3)
         */
        public AclBinding(
                final byte resourceType,
                final String resourceName,
                final byte resourcePatternType,
                final String principal,
                final String host,
                final byte operation,
                final byte permissionType) {
            this.resourceType = resourceType;
            this.resourceName = resourceName;
            this.resourcePatternType = resourcePatternType;
            this.principal = principal;
            this.host = host;
            this.operation = operation;
            this.permissionType = permissionType;
        }

        /**
         * Returns the Kafka resource type code.
         *
         * @return the resource type code
         */
        public byte resourceType() {
            return resourceType;
        }

        /**
         * Returns the name of the resource.
         *
         * @return the resource name
         */
        public String resourceName() {
            return resourceName;
        }

        /**
         * Returns the pattern type code.
         *
         * @return the resource pattern type code
         */
        public byte resourcePatternType() {
            return resourcePatternType;
        }

        /**
         * Returns the principal.
         *
         * @return the principal
         */
        public String principal() {
            return principal;
        }

        /**
         * Returns the host filter.
         *
         * @return the host
         */
        public String host() {
            return host;
        }

        /**
         * Returns the ACL operation code.
         *
         * @return the operation code
         */
        public byte operation() {
            return operation;
        }

        /**
         * Returns the permission type code.
         *
         * @return the permission type code
         */
        public byte permissionType() {
            return permissionType;
        }
    }

    /** The set of stored ACL bindings. */
    private final Set<AclBinding> bindings = ConcurrentHashMap.newKeySet();

    /**
     * Adds an ACL binding to the store.
     *
     * <p>If the binding already exists (by value equality), the store is unchanged.</p>
     *
     * @param binding the binding to add
     */
    public void addBinding(final AclBinding binding) {
        bindings.add(binding);
        log.info("Added ACL binding: {}", binding);
    }

    /**
     * Returns all stored ACL bindings.
     *
     * @return an unmodifiable snapshot of all bindings
     */
    public List<AclBinding> getBindings() {
        return List.copyOf(bindings);
    }

    /**
     * Removes and returns all bindings that match the given filter criteria.
     *
     * <p>A filter field value of {@code -1} (for byte fields) or {@code null}
     * (for string fields) is treated as a wildcard that matches any value.</p>
     *
     * @param resourceType        the resource type filter, or {@code -1} for any
     * @param resourceName        the resource name filter, or {@code null} for any
     * @param resourcePatternType the pattern type filter, or {@code -1} for any
     * @param principal           the principal filter, or {@code null} for any
     * @param host                the host filter, or {@code null} for any
     * @param operation           the operation filter, or {@code -1} for any
     * @param permissionType      the permission type filter, or {@code -1} for any
     * @return the list of bindings that were removed
     */
    public List<AclBinding> deleteMatchingBindings(
            final byte resourceType,
            final String resourceName,
            final byte resourcePatternType,
            final String principal,
            final String host,
            final byte operation,
            final byte permissionType) {
        final var matched = findMatchingBindings(
            resourceType, resourceName, resourcePatternType,
            principal, host, operation, permissionType);
        bindings.removeAll(matched);
        log.info("Deleted {} matching ACL bindings", matched.size());
        return matched;
    }

    /**
     * Finds all bindings that match the given filter criteria without removing them.
     *
     * <p>A filter field value of {@code -1} (for byte fields) or {@code null}
     * (for string fields) is treated as a wildcard that matches any value.</p>
     *
     * @param resourceType        the resource type filter, or {@code -1} for any
     * @param resourceName        the resource name filter, or {@code null} for any
     * @param resourcePatternType the pattern type filter, or {@code -1} for any
     * @param principal           the principal filter, or {@code null} for any
     * @param host                the host filter, or {@code null} for any
     * @param operation           the operation filter, or {@code -1} for any
     * @param permissionType      the permission type filter, or {@code -1} for any
     * @return the list of matching bindings
     */
    public List<AclBinding> findMatchingBindings(
            final byte resourceType,
            final String resourceName,
            final byte resourcePatternType,
            final String principal,
            final String host,
            final byte operation,
            final byte permissionType) {
        return bindings.stream()
            .filter(b -> matches(b, resourceType, resourceName,
                resourcePatternType, principal, host, operation, permissionType))
            .collect(Collectors.toList());
    }

    /**
     * Kafka wire-protocol code for the {@code ANY} enum value used in
     * {@code ResourceType}, {@code PatternType}, {@code AclOperation}, and
     * {@code AclPermissionType}. When a filter field carries this value it
     * matches any stored binding.
     */
    private static final byte ANY_CODE = 1;

    /**
     * Kafka wire-protocol code for {@code PatternType.MATCH}, which also acts
     * as a wildcard in ACL filter requests.
     */
    private static final byte MATCH_CODE = 4;

    /**
     * Tests whether a single binding matches all the given filter criteria.
     *
     * @param binding             the binding to test
     * @param resourceType        the resource type filter
     * @param resourceName        the resource name filter
     * @param resourcePatternType the pattern type filter
     * @param principal           the principal filter
     * @param host                the host filter
     * @param operation           the operation filter
     * @param permissionType      the permission type filter
     * @return {@code true} if the binding matches all non-wildcard criteria
     */
    private static boolean matches(
            final AclBinding binding,
            final byte resourceType,
            final String resourceName,
            final byte resourcePatternType,
            final String principal,
            final String host,
            final byte operation,
            final byte permissionType) {
        return isByteWildcardOrEqual(resourceType, binding.resourceType())
            && (resourceName == null || binding.resourceName().equals(resourceName))
            && isPatternTypeMatch(resourcePatternType, binding.resourcePatternType())
            && (principal == null || binding.principal().equals(principal))
            && (host == null || binding.host().equals(host))
            && isByteWildcardOrEqual(operation, binding.operation())
            && isByteWildcardOrEqual(permissionType, binding.permissionType());
    }

    /**
     * Returns {@code true} if the filter value is a wildcard ({@code -1} or
     * {@link #ANY_CODE}) or equals the binding value.
     *
     * @param filter  the filter value
     * @param binding the stored binding value
     * @return {@code true} if the filter matches
     */
    private static boolean isByteWildcardOrEqual(final byte filter, final byte binding) {
        return filter == -1 || filter == ANY_CODE || filter == binding;
    }

    /**
     * Returns {@code true} if the pattern type filter is a wildcard ({@code -1},
     * {@link #ANY_CODE}, or {@link #MATCH_CODE}) or equals the binding value.
     *
     * @param filter  the pattern type filter value
     * @param binding the stored pattern type value
     * @return {@code true} if the filter matches
     */
    private static boolean isPatternTypeMatch(final byte filter, final byte binding) {
        return filter == -1 || filter == ANY_CODE || filter == MATCH_CODE || filter == binding;
    }
}
