# Future plans

The following is a loose list of the things I'd like to add to Kafkaesque in roughly priority order

 * Proper example application that uses Kafka with a demo of the use of Kafkaesque
 * Nice landing page on the `kafkaesque.eu` domain
 * More helper methods for test suites (but which ones would be useful? Do we go full DSL?)
 * Various security stuff
   * mTLS
   * SASL/PLAIN
   * SASL_SSL
   * SASL/SCRAM (SCRAM-SHA-256 / SCRAM-SHA-512)
   * SASL/GSSAPI
   * SASL/OAUTHBEARER
   * ACLs or other authorisation
   * Anything else people are actually using!
 * Support older versions of Apache Client (including some pretty old ones)
 * Support older versions of Java
 * Error/Fault Injection DSL (... whatever that means; I need to think about it!)
 * DSL for wiring expectations on what gets published in tests
 * Some example tests combining Kafkaesque with Schema Registry (or maybe even a mock Schema Registry as part of Kafkaesque!)
