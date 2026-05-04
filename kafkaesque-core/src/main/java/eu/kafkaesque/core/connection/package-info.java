/**
 * Wire-protocol client connection management: socket I/O, request framing, and
 * response dispatch.
 *
 * <p>Parameters and return values in this package are non-null by default.
 * Use {@link edu.umd.cs.findbugs.annotations.Nullable} to opt out for
 * individual fields, parameters, or method returns where {@code null}
 * carries protocol semantics.</p>
 */
@ParametersAreNonnullByDefault
@ReturnValuesAreNonnullByDefault
package eu.kafkaesque.core.connection;

import javax.annotation.ParametersAreNonnullByDefault;

import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;
