# Standalone Server & Docker

Kafkaesque can run as a standalone service via Docker or as an executable JAR. This is useful
when you want a lightweight mock Kafka broker available over the network — for example, during
local development, in CI pipelines, or in Docker Compose stacks alongside your application.

## Running with Docker

### Using the pre-built image

A pre-built image is published to GitHub Container Registry on each release:

```bash
docker pull ghcr.io/dcminter/kafkaesque:latest
docker run -p 9092:9092 ghcr.io/dcminter/kafkaesque:latest
```

You can also pin to a specific version:

```bash
docker pull ghcr.io/dcminter/kafkaesque:0.1.0
```

### Building the image locally

From the repository root:

```bash
docker build -t kafkaesque .
docker run -p 9092:9092 kafkaesque
```

Your Kafka clients can then connect to `localhost:9092`.

### Configuration

The following environment variables control the server:

| Variable                         | Default   | Description                                   |
|----------------------------------|-----------|-----------------------------------------------|
| `KAFKAESQUE_HOST`                | `0.0.0.0` | Bind address                                  |
| `KAFKAESQUE_PORT`                | `9092`    | Listen port                                   |
| `KAFKAESQUE_AUTO_CREATE_TOPICS`  | `true`    | Allow producers to auto-create topics         |
| `KAFKAESQUE_LOG_LEVEL`           | `INFO`    | Logging level (`DEBUG`, `INFO`, `WARN`, etc.) |

Example with custom settings:

```bash
docker run -p 19092:19092 \
  -e KAFKAESQUE_PORT=19092 \
  -e KAFKAESQUE_AUTO_CREATE_TOPICS=false \
  -e KAFKAESQUE_LOG_LEVEL=DEBUG \
  kafkaesque
```

### Docker Compose

Using the pre-built image:

```yaml
services:
  kafkaesque:
    image: ghcr.io/dcminter/kafkaesque:latest
    ports:
      - "9092:9092"
    environment:
      KAFKAESQUE_AUTO_CREATE_TOPICS: "true"
```

Or building from source:

```yaml
services:
  kafkaesque:
    build: .
    ports:
      - "9092:9092"
    environment:
      KAFKAESQUE_AUTO_CREATE_TOPICS: "true"
```

## Running as an executable JAR

Build the project and run the fat JAR directly:

```bash
./mvnw -pl kafkaesque-standalone -am -DskipTests package
java -jar kafkaesque-standalone/target/kafkaesque-standalone-0.1.0-SNAPSHOT.jar
```

The same environment variables listed above apply when running the JAR directly.

## Limitations

The standalone server provides the same mock Kafka implementation as the library — it is
intended for testing and development, not production use. See the main
[README](../README.md) for details on when Kafkaesque is (and isn't) a good fit.
