#!/usr/bin/env bash
set -e

MVN="./mvnw --batch-mode --no-transfer-progress -Dlog.level=WARN"

echo "=== Building full project with Java 11 ==="
$MVN clean install

echo "=== Compile-only pass for each Kafka client version ==="
$MVN -pl kafkaesque-it clean test-compile -Dkafka.clients.test.version=1.0.2 -Dkafka.api.level=1
$MVN -pl kafkaesque-it clean test-compile -Dkafka.clients.test.version=1.1.1 -Dkafka.api.level=1
$MVN -pl kafkaesque-it clean test-compile -Dkafka.clients.test.version=2.0.1 -Dkafka.api.level=1
$MVN -pl kafkaesque-it clean test-compile -Dkafka.clients.test.version=2.8.2 -Dkafka.api.level=1
$MVN -pl kafkaesque-it clean test-compile -Dkafka.clients.test.version=3.0.2 -Dkafka.api.level=1
$MVN -pl kafkaesque-it clean test-compile -Dkafka.clients.test.version=3.5.2 -Dkafka.api.level=3
$MVN -pl kafkaesque-it clean test-compile -Dkafka.clients.test.version=4.0.1 -Dkafka.api.level=4
$MVN -pl kafkaesque-it clean test-compile -Dkafka.clients.test.version=4.2.0 -Dkafka.api.level=4

echo "=== Verify pass for each Kafka client version ==="
$MVN -pl kafkaesque-it clean verify -Dkafka.clients.test.version=1.0.2 -Dkafka.api.level=1
$MVN -pl kafkaesque-it clean verify -Dkafka.clients.test.version=1.1.1 -Dkafka.api.level=1
$MVN -pl kafkaesque-it clean verify -Dkafka.clients.test.version=2.0.1 -Dkafka.api.level=1
$MVN -pl kafkaesque-it clean verify -Dkafka.clients.test.version=2.8.2 -Dkafka.api.level=1
$MVN -pl kafkaesque-it clean verify -Dkafka.clients.test.version=3.0.2 -Dkafka.api.level=1
$MVN -pl kafkaesque-it clean verify -Dkafka.clients.test.version=3.5.2 -Dkafka.api.level=3
$MVN -pl kafkaesque-it clean verify -Dkafka.clients.test.version=4.0.1 -Dkafka.api.level=4
$MVN -pl kafkaesque-it clean verify -Dkafka.clients.test.version=4.2.0 -Dkafka.api.level=4

echo "=== All builds completed successfully ==="
