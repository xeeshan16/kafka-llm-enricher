# syntax=docker/dockerfile:1.7
# Multi-stage build of the standalone enricher (enricher-app).
#   docker build -t kafka-llm-enricher:local .
# Tests are not run here; CI runs them before it builds this image.

FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /src
COPY .mvn/ .mvn/
COPY pom.xml ./
COPY enricher-core/pom.xml enricher-core/
COPY enricher-ledger-postgres/pom.xml enricher-ledger-postgres/
COPY enricher-kafka/pom.xml enricher-kafka/
COPY enricher-spring-boot-starter/pom.xml enricher-spring-boot-starter/
COPY enricher-app/pom.xml enricher-app/
COPY enricher-core/src enricher-core/src
COPY enricher-ledger-postgres/src enricher-ledger-postgres/src
COPY enricher-kafka/src enricher-kafka/src
COPY enricher-spring-boot-starter/src enricher-spring-boot-starter/src
COPY enricher-app/src enricher-app/src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -pl enricher-app -am package \
      -DskipTests -Dspotless.check.skip=true -Djacoco.skip=true \
 && cp enricher-app/target/enricher-app-*.jar /src/app.jar \
 && java -Djarmode=tools -jar /src/app.jar extract --layers --launcher --destination /src/extracted

FROM eclipse-temurin:21-jre-noble
LABEL org.opencontainers.image.title="kafka-llm-enricher" \
      org.opencontainers.image.description="Exactly-once, budgeted LLM enrichment for Kafka streams" \
      org.opencontainers.image.source="https://github.com/xeeshan16/kafka-llm-enricher" \
      org.opencontainers.image.licenses="MIT"
WORKDIR /app
# Dependencies change least often, the application most: one layer each.
COPY --from=build /src/extracted/dependencies/ ./
COPY --from=build /src/extracted/spring-boot-loader/ ./
COPY --from=build /src/extracted/snapshot-dependencies/ ./
COPY --from=build /src/extracted/application/ ./
USER 10001:10001
EXPOSE 8080
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
