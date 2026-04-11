# Future plans

The following is a loose list of the things I'd like to add to Kafkaesque in roughly priority order

 * Proper example application that uses Kafka with a demo of the use of Kafkaesque
 * Web pages! (perhaps added to the standalone Docker image)
 * A proper release pipeline
 * More helper methods for test suites (but which ones would be useful? Do we go full DSL?)
 * Various security stuff
  * mTLS
  * SASL/PLAIN
  * SASL_SSL
  * SASL/SCRAM (SCRAM-SHA-256 / SCRAM-SHA-512)
  * SASL/GSSAPI
  * SASL/OAUTHBEARER
  * ACLs or other authorisation
 * Error/Fault Injection DSL (... whatever that means; I need to think about it!)
 * Multi-version ... support all the major Kafka client versions (including some pretty old ones)
 * DSL for wiring expectations on what gets published in tests (use planning mode to get this clearer defined)
 * Some example tests combining Kafkaesque with Schema Registry (or maybe even a mock Schema Registry as part of Kafkaesque!)
