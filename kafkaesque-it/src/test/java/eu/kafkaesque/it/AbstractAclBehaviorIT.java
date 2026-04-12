package eu.kafkaesque.it;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract base class defining the ACL admin-client integration test suite.
 *
 * <p>Covers creation, description, and deletion of ACL bindings via
 * {@link AdminClient}. Subclasses must implement {@link #getBootstrapServers()}.</p>
 */
@Slf4j
abstract class AbstractAclBehaviorIT {

    /**
     * Returns the bootstrap servers string for the Kafka backend under test.
     *
     * @return bootstrap servers in {@code "host:port"} format
     * @throws Exception if the address cannot be determined
     */
    protected abstract String getBootstrapServers() throws Exception;

    /**
     * Verifies that an ACL binding can be created via {@link AdminClient#createAcls}
     * and then retrieved via {@link AdminClient#describeAcls}.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldCreateAndDescribeAcls() throws Exception {
        // Given
        final var topicName = "test-acl-topic-" + UUID.randomUUID();
        final var resourcePattern = new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL);
        final var ace = new AccessControlEntry("User:test", "*", AclOperation.READ, AclPermissionType.ALLOW);
        final var binding = new AclBinding(resourcePattern, ace);

        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            // When
            adminClient.createAcls(of(binding)).all().get();

            // Then
            final Collection<AclBinding> acls = adminClient.describeAcls(AclBindingFilter.ANY).values().get();
            final var matchingAcls = acls.stream()
                .filter(a -> a.pattern().name().equals(topicName))
                .collect(Collectors.toList());

            assertThat(matchingAcls).hasSize(1);
            final var found = matchingAcls.get(0);
            assertThat(found.pattern().resourceType()).isEqualTo(ResourceType.TOPIC);
            assertThat(found.pattern().patternType()).isEqualTo(PatternType.LITERAL);
            assertThat(found.entry().principal()).isEqualTo("User:test");
            assertThat(found.entry().host()).isEqualTo("*");
            assertThat(found.entry().operation()).isEqualTo(AclOperation.READ);
            assertThat(found.entry().permissionType()).isEqualTo(AclPermissionType.ALLOW);

            log.info("Created and described ACL for topic {}", topicName);
        }
    }

    /**
     * Verifies that an ACL binding can be deleted via {@link AdminClient#deleteAcls}
     * and that it no longer appears in a subsequent describe call.
     *
     * @throws Exception if the admin client or bootstrap address lookup fails
     */
    @Test
    void shouldDeleteAcls() throws Exception {
        // Given
        final var topicName = "test-acl-delete-topic-" + UUID.randomUUID();
        final var resourcePattern = new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL);
        final var ace = new AccessControlEntry("User:test", "*", AclOperation.READ, AclPermissionType.ALLOW);
        final var binding = new AclBinding(resourcePattern, ace);

        final var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createAcls(of(binding)).all().get();

            // When
            final var deleteFilter = new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL),
                new AccessControlEntryFilter("User:test", "*", AclOperation.READ, AclPermissionType.ALLOW));
            adminClient.deleteAcls(of(deleteFilter)).all().get();

            // Then
            final Collection<AclBinding> acls = adminClient.describeAcls(AclBindingFilter.ANY).values().get();
            final var matchingAcls = acls.stream()
                .filter(a -> a.pattern().name().equals(topicName))
                .collect(Collectors.toList());

            assertThat(matchingAcls).isEmpty();

            log.info("Deleted ACL for topic {}", topicName);
        }
    }
}
