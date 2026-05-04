/**
 * Broker-internal pub/sub for record-published, topic-created, and transaction
 * lifecycle notifications.
 *
 * <p>Parameters and return values in this package are non-null by default.
 * Use {@link edu.umd.cs.findbugs.annotations.Nullable} to opt out for
 * individual fields, parameters, or method returns where {@code null}
 * is meaningful.</p>
 */
@ParametersAreNonnullByDefault
@ReturnValuesAreNonnullByDefault
package eu.kafkaesque.core.listener;

import javax.annotation.ParametersAreNonnullByDefault;

import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;
