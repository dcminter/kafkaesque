# Stage 1: Build the standalone fat JAR
FROM eclipse-temurin:11-jdk-noble AS build

WORKDIR /build

# Copy Maven wrapper and project files
COPY .mvn/ .mvn/
COPY mvnw pom.xml checkstyle.xml ./
COPY kafkaesque-bom/pom.xml kafkaesque-bom/pom.xml
COPY kafkaesque-core/ kafkaesque-core/
COPY kafkaesque-junit4/pom.xml kafkaesque-junit4/pom.xml
COPY kafkaesque-junit4/src/ kafkaesque-junit4/src/
COPY kafkaesque-junit5/pom.xml kafkaesque-junit5/pom.xml
COPY kafkaesque-junit5/src/ kafkaesque-junit5/src/
COPY kafkaesque-standalone/ kafkaesque-standalone/
COPY kafkaesque-it/pom.xml kafkaesque-it/pom.xml

# Build only the standalone module and its dependencies, skip tests
RUN --mount=type=cache,target=/root/.m2/repository \
    ./mvnw --batch-mode --no-transfer-progress \
    -pl kafkaesque-standalone -am \
    -DskipTests \
    package

# Stage 2: Minimal runtime image
FROM eclipse-temurin:11-jre-noble

LABEL org.opencontainers.image.title="Kafkaesque" \
      org.opencontainers.image.description="A mock Kafka service for testing" \
      org.opencontainers.image.source="https://github.com/dcminter/kafkaesque"

RUN groupadd --system kafkaesque && useradd --system --gid kafkaesque kafkaesque

WORKDIR /app

COPY --from=build /build/kafkaesque-standalone/target/kafkaesque-standalone-*.jar kafkaesque.jar

RUN chown -R kafkaesque:kafkaesque /app

USER kafkaesque

EXPOSE 9092

ENV KAFKAESQUE_HOST=0.0.0.0 \
    KAFKAESQUE_PORT=9092 \
    KAFKAESQUE_AUTO_CREATE_TOPICS=true

ENTRYPOINT ["java", "-jar", "kafkaesque.jar"]
