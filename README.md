# Kafkaesque

A library for mocking Kafka dependencies in a realistic way

## Why not just use Kafka?

While running Kafka itself (perhaps within [TestContainers](https://testcontainers.com/modules/kafka/)) is a perfectly reasonable approach, it does have some drawbacks - depending on how you configure and launch it, it can be slow, perhaps taking multiple seconds to start up in a naiive configuration. If you're currently using Kafka in your integration tests and have no problems, then Kafkaesque is probably
not the tool for you.

If you're finding your Kafka tests are very slow (particularly if they launch large numbers of Kafka instances during 
the test lifecycle), or you want more control over the exact behaviours you're testing for, then Kafkaesque might be 
a good fit. It also might work for you if running Kafka inside testcontainers creates a dependency on Docker that 
would otherwise be unnecessary.

Note that if your tests are very slow because you're inserting `sleep` statements into otherwise fragile tests of asynchronous behaviour, then you might alternatively/additionally want to investigate the excellent [Awaitility library](http://www.awaitility.org/).

## Building and testing

The build tool is [Maven](https://maven.apache.org/) and we're using [Maven Wrapper](https://maven.apache.org/tools/wrapper/)
so to build and run the test suite:

```bash
$ ./mvnw clean verify
```

## License & Development

The software [is licensed under the Apache License, Version 2.0](LICENSE.txt)

This software is designed to support projects making extensive use of Apache Kafka. It depends on
Apache Kafka libraries for its wire-protocol types, and it therefore makes sense to release it under the
same license.

## AI Declaration

Large parts of this software were developed using [Claude Code](https://code.claude.com/)

## Internals

See [the event storage summary](EVENT_STORAGE_SUMMARY.md) for details of the internal representation of events etc.