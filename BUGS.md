# Known issues

This file tracks bugs and contract inconsistencies that have been identified but are not
yet fixed. Each entry describes the symptom, the affected file(s) with line references where
useful, and a one-line note on the suggested direction for a fix.

New entries should be appended as they are discovered so they don't get lost in conversation
context or commit history.

## 1. Handler exceptions silently drop the response

Every `generateXxxResponse(...)` method on the API handlers wraps its body in
`try { ... } catch (final Exception e) { log.error(...); return null; }`. The `null` flows
to `KafkaProtocolHandler.writeResponse` (line 648), which silently skips the write. The
client (real Kafka clients on the other end of the wire) receives nothing and waits until
it times out.

**Affected files:** `AclApiHandler`, `ConsumerDataApiHandler`, `ClusterApiHandler`,
`ConsumerGroupApiHandler`, `ProducerApiHandler`, `AdminApiHandler`, `TransactionApiHandler`
(approximately 40 occurrences).

**Suggested fix:** return a protocol-level error response with the appropriate Kafka error
code, rather than dropping the response. The exact error code depends on the failure mode
(deserialisation error, internal failure, etc.).

## 2. Unhandled API key returns no response

`KafkaProtocolHandler.dispatchRequest` (line 434) logs `"Unhandled API key: ..."` and
returns `null`, which leads to a silent drop in `writeResponse`. The telemetry registration
in `registerTelemetryHandlers` (line 606) shows the right shape: route through
`clusterApiHandler.generateUnsupportedResponse(...)`.

**Suggested fix:** in `dispatchRequest`, when `handlers.get(apiKey) == null`, route through
`generateUnsupportedResponse` (or the equivalent) so the client receives a proper protocol
error instead of nothing.

## 3. `TransactionCoordinator.getProducerIdAndEpoch` contract inconsistency

The lookup's Javadoc states the method may return `null`, but its caller in
`TransactionApiHandler.java:354` wraps the call in `Objects.requireNonNull(...)`, asserting
it cannot. Either the Javadoc is wrong (the invariant guarantees non-null at this call
site) or there's a latent NPE waiting for a request with an unknown `transactionalId`.

**Suggested fix:** confirm the actual invariant. If non-null is guaranteed at the call site
(e.g. because `INIT_PRODUCER_ID` always runs first), tighten the lookup's Javadoc and use
`orElseThrow(() -> new IllegalStateException("..."))` with a clear diagnostic. If null is
genuinely reachable, return a protocol-level error to the client rather than throwing.
