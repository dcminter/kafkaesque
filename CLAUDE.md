# Claude's context:

The goal of this project is to build Kafkaesque, a library that provides a mock Kafka service for testing applications that have strong dependencies on Kafka's capabilities. It is analagous to Wiremock's provision of mocking APIs for applications that make use of web servers and services.

## Project overview

The is built with Maven and uses Java 25 (we may in future backport to a more typical earlier version of Java).

Several directories exist. They are:

 * kafkaesque-bom - This contains the "bill of materials" pom that defines all of the versions for dependencies of the project
 * kafkaesque-core - This will contain the core implementation classes of the library.
 * kafkaesque-it - This contains the integration tests (behavioural tests) for the library

The library is going to implement the wire protocol directly - it will NOT run a real instance of Kafka at any point; instead it will create its own sockets etc. however it WILL make use of the Kafka libraries to have access to Kafka's own data transfer types used in the Kafka wire protocol(s).

Ultimately we will provide the features of this library as DSL-like configuration and also as annotations & types compatible with JUnit 5 - just like Wiremock does for normal Http server stuff.

## Development standards

  * Immutability is encouraged wherever reasonably possible; all parameters, fields, and variables should therefore be declared `final` unless their mutability is essential.
  * Modern Java features are encouraged - for example prefer `record` types to classes for simple DTOs 
  * Lombok annotations are encouraged to keep boilerplate "noise" to a minimum
  * Where "external" infrastructure is needed to support integration tests then `testcontainers` should be used to support this (and Claude Code itself is being run within a Docker container so configuration of testcontainers must allow for this)
  * Keep method implementations fairly short - where there's opportunity for re-use, put these into private methods.
  * Public methods and classes SHOULD generally have corresponding unit tests
  * Prefer to take "configuration" or "context" classes or records over very long parameter lists on methods and constructors. 
  * Write proper javadoc comments for ALL classes
    * and for ALL non-private methods and fields (including @param, @return and @exception entries)
    * and CONSIDER providing proper javadoc comments for private methods and fields
  * Don't commit changes to git unless this is explicitly requested

