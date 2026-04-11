# Future plans

The following is a loose list of the things I'd like to add to Kafkaesque in roughly priority order

 * JUnit 4 / JUnit 5 Vintage support (Rule, ClassRule)
 * Proper example application that uses Kafka with a demo of the use of Kafkaesque
 * Web pages!
 * A proper release pipeline
 * More helper methods for test suites (but which ones would be useful?)
 * Listeners for direct access to lifecycle (published, consumed, etc.)
 * Various security stuff
  * mTLS
  * SASL/PLAIN
  * SASL_SSL
  * SASL/SCRAM (SCRAM-SHA-256 / SCRAM-SHA-512)
  * SASL/GSSAPI
  * SASL/OAUTHBEARER
  * ACLs or other authorisation
 * Error/Fault Injection DSL (what exactly do we mean here?)
 * Multi-version ... support all the major Kafka client versions (including some pretty old ones)
 * DSL for wiring expectations on what gets published in tests (use planning mode to get this clearer defined)
 * Some example tests combining Kafkaesque with Schema Registry (or maybe even a mock Schema Registry as part of Kafkaesque!)
