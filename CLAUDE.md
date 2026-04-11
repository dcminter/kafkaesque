# Claude's context:

The goal of this project is to build Kafkaesque, a library that provides a mock Kafka service for testing applications 
that have strong dependencies on Kafka's capabilities. It is analogous to Wiremock's provision of mocking APIs for 
applications that make use of web servers and other http based services.

## Project overview

The is built with Maven via Maven Wrapper and uses Java 25 (we may in future backport to a more typical earlier 
version of Java).

Several directories exist. They are:

 * kafkaesque-bom - This contains the "bill of materials" pom that defines all of the versions for dependencies of the project
 * kafkaesque-core - This will contain the core implementation classes of the library.
 * kafkaesque-it - This contains the integration tests (behavioural tests) for the library
 * kafkaesque-junit5 - This contains support (including annotations) for using the library in JUnit 5 (Jupiter) tests

The library implements the wire protocol directly - it does NOT run a real instance of Kafka at any point (outside of
the library's own test scenarios); instead it creates its own sockets etc. but it DOES make use of the Kafka 
libraries to have access to Kafka's own data transfer types used in the Kafka wire protocol(s).

We'll probably provide a small DSL to aid in writing test scenarios once we've worked out what we want to express 
with it! For now the JUnit 5 annotations should work well in existing test cases.

## Development standards

After making any edits, you must ensure `mvn checkstyle:check` passes with zero violations. If checkstyle fails, fix 
all reported violations before considering the task complete. This will enforce some, but not all, of the following:

  * Integration test cases must always be run against both Kafkaesque and the real Kafka brokers to verify that we're correctly implementing the Kafka line protocol
  * Immutability is encouraged wherever reasonably possible; all parameters, fields, and variables should therefore be declared `final` unless their mutability is essential.
  * Modern Java features are encouraged - for example prefer `record` types to classes for simple DTOs 
  * Lean towards declaring collection fields and parameters as interface types (Map, Set, etc) rather than concrete types (HashMap, HashSet, etc.)
    * i.e. prefer `private final Map<String,String> foo = ConcreteHashMap<>()` to `private final ConcreteHashMap<String,String> foo = ConcreteHashMap<>()`
    * But for local variables prefer `var`, i.e. `let var foo = ConcreteHashMap<String,String>()` form
  * Lean towards using static imports in order to keep things terse
  * Lombok annotations are encouraged to keep boilerplate "noise" to a minimum
  * Where "external" infrastructure is needed to support integration tests then `testcontainers` should be used to support this
  * Keep method implementations fairly short - where there's opportunity for re-use, put these into private methods.
  * Public methods and classes SHOULD generally have corresponding unit tests
    * The exception is for things like Enums or Records when they have no method implementations
  * Prefer to take "configuration" or "context" classes or records over very long parameter lists on methods and constructors. 
  * Write proper javadoc comments for ALL classes
    * and for ALL non-private methods and fields (including @param, @return and @exception entries)
    * and CONSIDER providing proper javadoc comments for private methods and fields
  * Don't commit changes to git unless this is explicitly requested
  * Don't disable any test cases (including new ones) unless you get explicit approval
  * There are currently no warnings in the build output - if changes cause new warnings, the warnings should be addressed
