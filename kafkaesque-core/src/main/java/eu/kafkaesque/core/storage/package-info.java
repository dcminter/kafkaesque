/**
 * In-memory storage of broker state: topics, records, ACLs, and transaction state.
 *
 * <p>Parameters and return values in this package are non-null by default.
 * Use {@link edu.umd.cs.findbugs.annotations.Nullable} to opt out for
 * individual fields, parameters, or method returns where {@code null}
 * carries protocol or {@code Map}-API semantics.</p>
 */
@ParametersAreNonnullByDefault
@ReturnValuesAreNonnullByDefault
package eu.kafkaesque.core.storage;

import javax.annotation.ParametersAreNonnullByDefault;

import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;
