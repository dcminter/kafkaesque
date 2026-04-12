# Future plans

The following is a loose list of the things I'd like to add to Kafkaesque in roughly priority order

 * ~~Support older versions of Apache Client~~ (done — kafka-clients is shaded; users bring their own 1.x-4.x)
 * ~~CI matrix testing across kafka-clients versions (1.x, 2.x, 3.x, 4.x)~~ (done — CI matrix tests 1.0.2, 1.1.1, 2.0.1, 2.8.2, 3.0.2, 3.5.2, 4.0.1, 4.2.0)
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
 * Error/Fault Injection DSL (... whatever that means; I need to think about it!)
 * DSL for wiring expectations on what gets published in tests
 * Some example tests combining Kafkaesque with Schema Registry (or maybe even a mock Schema Registry as part of Kafkaesque!)
