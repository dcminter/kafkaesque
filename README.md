# Kafkaesque

A library for mocking Kafka dependencies

## Building and testing

The build tool is [Maven](https://maven.apache.org/) and we're using [Maven Wrapper](https://maven.apache.org/tools/wrapper/) 
so to build and run the test suite:

```bash
$ ./mvnw clean verify
```

## Internals

See [the event storage summary](EVENT_STORAGE_SUMMARY.md) for details of the internal representation of events etc.