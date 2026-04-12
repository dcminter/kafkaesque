# Multi-version Kafka Client Support

Kafkaesque shades its own copy of the Apache Kafka protocol classes (relocated to
`eu.kafkaesque.shaded.org.apache.kafka`) so that it does not conflict with whatever
version of `kafka-clients` the user has on their classpath.

The JUnit modules (`kafkaesque-junit5`, `kafkaesque-junit4`) declare `kafka-clients`
at `provided` scope, meaning users must supply their own version. The factory methods
and annotations in these modules use stable Kafka client types (`KafkaProducer`,
`KafkaConsumer`, `Serializer`, `Deserializer`) that have been present since
`kafka-clients` 1.x. The `@KafkaesqueConsumer` annotation uses a Kafkaesque-defined
`IsolationLevel` enum rather than the Kafka-internal one (which moved packages across
major releases).

## Supported range

Kafkaesque targets compatibility with `kafka-clients` **1.x through 4.x**.

The following versions are candidates for CI matrix testing:

  * 1.x: 1.0.2, 1.1.1
  * 2.x: 2.0.1, 2.8.2
  * 3.x: 3.0.2, 3.5.2, 3.9.2
  * 4.x: 4.0.1, 4.2.0

## Testing with different kafka-clients versions

Two Maven properties control multi-version testing:

  * `kafka.clients.test.version` — which version of `kafka-clients` the JUnit and
    integration test modules compile and test against. Defaults to the same version
    used internally by `kafkaesque-core` (currently 3.9.2).
  * `kafka.api.level` — which tier of integration tests to compile. Defaults to `3`.

### API level tiers

The integration tests are split into three source directories, compiled conditionally:

| `kafka.api.level` | Test tiers compiled                      | Suitable for              |
|--------------------|------------------------------------------|---------------------------|
| `1`                | Base (producer, consumer, transactions)  | kafka-clients 1.x – 3.4  |
| `2`                | Base (same as level 1)                   | kafka-clients 2.x – 3.4  |
| `3` (default)      | Base + Admin + Deprecated Admin          | kafka-clients 3.5 – 3.x  |
| `4`                | Base + Admin (no deprecated)             | kafka-clients 4.x+        |

  * **Base** (`src/test/java`): Producer, consumer, compression, compaction, retention,
    transaction, and idempotent producer tests. Uses only APIs available since
    `kafka-clients` 1.x.
  * **Admin** (`src/test/java-admin`): Admin client tests covering topic management,
    ACLs, consumer groups, transactions, and incremental config changes. Uses APIs
    introduced in `kafka-clients` 2.x/3.x that are not available in older versions.
  * **Deprecated Admin** (`src/test/java-deprecated-admin`): Tests for the deprecated
    `AdminClient.alterConfigs()` API, which was removed in `kafka-clients` 4.0.

### Recommended test commands

```bash
# kafka-clients 1.x through 3.4.x (base tests only)
./mvnw clean verify -Dkafka.clients.test.version=1.1.1 -Dkafka.api.level=1
./mvnw clean verify -Dkafka.clients.test.version=2.8.2 -Dkafka.api.level=1
./mvnw clean verify -Dkafka.clients.test.version=3.0.2 -Dkafka.api.level=1

# kafka-clients 3.5+ (default — all tests)
./mvnw clean verify -Dkafka.clients.test.version=3.5.2
./mvnw clean verify -Dkafka.clients.test.version=3.9.2

# kafka-clients 4.x+ (base + admin, no deprecated)
./mvnw clean verify -Dkafka.clients.test.version=4.0.1 -Dkafka.api.level=4
./mvnw clean verify -Dkafka.clients.test.version=4.2.0 -Dkafka.api.level=4
```

CI runs a matrix job that tests against all candidate versions listed above.

## How it works

  * `kafkaesque-core` uses the `maven-shade-plugin` to embed `kafka-clients` with all
    `org.apache.kafka` packages relocated to `eu.kafkaesque.shaded.org.apache.kafka`.
  * The dependency-reduced POM ensures that downstream consumers of `kafkaesque-core`
    do **not** inherit a transitive dependency on `kafka-clients`.
  * The JUnit modules compile against `kafka-clients` but declare it as `provided`.
    Users add their own version to the test classpath.
  * The integration test module (`kafkaesque-it`) uses `build-helper-maven-plugin` to
    conditionally add extra test source directories based on `kafka.api.level`. Maven
    profiles set the plugin execution phase to `none` when a tier should be skipped.

## Historical test results

Tested on 2026-04-12 against commit `cc65e8d` (before the tiered test fix was applied):

| kafka-clients | core | junit4 | junit5 | IT (kafkaesque-it) |
|--------------|------|--------|--------|-------------------|
| **1.1.1** | OK | OK | **FAIL** (compile) | **FAIL** (compile) |
| **2.0.1** | OK | OK | **FAIL** (compile) | not tested |
| **2.8.2** | OK | OK | OK | **FAIL** (compile) |
| **3.0.2** | OK | OK | OK | **FAIL** (compile) |
| **3.5.2** | OK | OK | OK | 111/112 pass (1 timeout) |
| **3.9.2** | OK | OK | OK | 112/112 pass |
| **4.0.1** | OK | OK | OK | **FAIL** (compile) |
| **4.2.0** | OK | OK | OK | **FAIL** (compile) |

All failures were in test code, not in `kafkaesque-core`. The root causes and fixes:

  * **junit5 + 1.x/2.0.x** — `@KafkaesqueConsumer` referenced
    `org.apache.kafka.common.IsolationLevel`, which moved packages across Kafka releases.
    Fixed by replacing it with a Kafkaesque-defined `eu.kafkaesque.junit5.IsolationLevel`
    enum.
  * **IT + <3.5** — Admin behaviour tests used classes (`TransactionListing`,
    `AlterConfigOp`, `OffsetSpec`, `PatternType`, etc.) introduced after kafka-clients
    2.8.x. Fixed by moving admin tests to `src/test/java-admin/`, compiled only when
    `kafka.api.level >= 3`.
  * **IT + 4.x** — `AdminClient.alterConfigs()` was removed in kafka-clients 4.0. Fixed
    by extracting the deprecated-API test into `src/test/java-deprecated-admin/`, compiled
    only when `kafka.api.level = 3`.
  * **3.5.2 timeout** — `shouldDescribeConsumerGroupViaAdminClient` timed out, likely a
    protocol version negotiation issue (not a compilation problem).
