# Kafkaesque

A library for mocking Kafka dependencies

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