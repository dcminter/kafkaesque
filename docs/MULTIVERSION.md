# Multi-version

Kafkaesque supports multiple versions of the Apache Kafka client library `org.apache.kafka:kafka-clients`. Specifically
it is tested against versions `1.0.2`, `1.1.1`, `2.0.1`, `2.8.2`, `3.0.2`, `3.5.2`, `4.0.1`, and `4.2.0`. Collectively
these cover the major changes and additions of functionality in Apache Kafka introduced from the point at which the
library supported Java 11.

From an end user's point of view, you should be able to use the same version of Apache Kafka in your integration
tests as your application uses, and have all three running in the same process as the Kafkaesque server with no
issues. If you do find an issue, please submit it to [the Github repo](https://github.com/dcminter/kafkaesque/issues) 
with a test-case that reproduces it for us (you can submit without a test case, but you're more likely to get a quick 
fix if you do provide one).

## Architecture

The `kafkaesque-core` library is implemented by using the Apache Kafka client library DTOs directly so that we don't 
have to roll our own implementation of the Kafka wire protocol from scratch. Currently the version of the client library
used in the core library is 3.9.1 - but this brings a problem if the test suite and/or the application under test
is going to be using a different version of the client library.  We therefore shade (copy) the Apache Kafka client 
classes into the `kafkaesque-core` library, renaming their packages from `org.apache.kafka` to 
`eu.kafkaesque.shaded.org.apache.kafka` in the process.

End users therefore provide their own Apache Kafka client library when implementing test suites with the Kafkaesque 
responding appropriately to their API calls.

Because earlier client implementations offered less (or in some cases different) functionality than later ones, and 
the integration test suite must be run with, and cover, all of the supported client versions, there is some 
partitioning of the Kafkaesque integration tests. They are broadly divided into four API "levels" with the following
test relationship:

| API Level | Test directories                                         | Target versions    |
|-----------|----------------------------------------------------------|--------------------|
| 1         | `src/java/test` only                                     | Kafka 1.x to 3.0.x |
| 2         | `src/test/java` only                                     | Not used           |
| 3         | `src/test/java` & `java-admin` & `java-deprecated-admin` | Kafka 3.5.x        |
| 4         | `src/test/java` & `java-admin`                           | Kafka 4.x+         |

Probably we will add more versions as new issues (from the point of view of our implementation) versions appear. The
application of these inclusions and exclusions is applied through the `build-helper-maven-plugin` with the
`kafka.clients.test.version` system property determining which Kafka client version will be applied and the 
`kafka.api.level` system property (defaulting to `3`) can be used to specify which should apply at build time.

The script `scripts/build-all-versions.sh` can be used to each of the supported Kafka client versions with an 
appropriate API level. This script is time consuming because it:

  * Builds the entire suite at the default `3.9.1` level (to ensure the appropriate core module has been built with the shaded client library and that all tests pass at the default level)
  * For each supported version in turn, compiles and runs unit tests for the `kafkaesque-it` module at the appropriate API level
    * This is relatively quick and ensures that the most egregious incompatibilities are discovered swiftly
  * For each supported version in turn, compiles and runs unit tests and the full integration test suite at the appropriate API level
    * This is quite slow, but ensures that the core and unit test libraries will fully provide the behaviour needed for each of the supported client versions

Unfortunately the integration tests include tests for things like correct timeout behaviour and other intrinsically 
slow operations. We thus provide a library whose goal is to speed up your integration tests but whose own integration
test speed is dismal. Sometimes you just can't win.

## A note about "Support"

While I've laid out the specific versions that Kafkaesque "supports" in this document and others, we're not expecting
other intermediate versions to have particular issues. If you discover some, please do raise issues and I'll take a 
look. It's just that these are the only versions that I'm currently specifically testing against. If we see 
regressions that are unique to specific other versions, we'll add them to the test suite and then they'll be
"supported" in the same sense.