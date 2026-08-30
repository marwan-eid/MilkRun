# ═══════════════════════════════════════════════════════════
# Multi-stage Dockerfile for The Milk-Run Backend
# Stage 1: Build with Maven
# Stage 2: JVM runtime (slim)
# Stage 3: GraalVM native image (optional, via --target=native)
# ═══════════════════════════════════════════════════════════

# ──── Stage 1: Build ────
FROM eclipse-temurin:21-jdk-jammy AS builder

WORKDIR /app
COPY pom.xml .
COPY .mvn .mvn
COPY mvnw .

# Download dependencies first (Docker layer caching)
RUN mvn dependency:go-offline -B 2>/dev/null || true

COPY src src
RUN mvn clean package -DskipTests -q

# ──── Stage 2: JVM Runtime (default) ────
FROM eclipse-temurin:21-jre-jammy AS jvm

LABEL maintainer="milkrun-team"
LABEL description="The Milk-Run Backend - Reactive Fleet Tracking Engine"

WORKDIR /app

COPY --from=builder /app/target/*.jar app.jar

# JVM tuning for containers
ENV JAVA_OPTS="-XX:+UseZGC \
    -XX:MaxRAMPercentage=75.0 \
    -XX:+UseStringDeduplication \
    -Djava.security.egd=file:/dev/./urandom"

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:8080/api/observability/live || exit 1

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]

# ──── Stage 3: GraalVM Native Image ────
FROM ghcr.io/graalvm/native-image-community:21 AS native-builder

WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar

RUN native-image \
    --no-fallback \
    -H:+ReportExceptionStackTraces \
    --initialize-at-build-time=org.slf4j \
    -jar app.jar \
    -o milkrun-native

FROM debian:bookworm-slim AS native

LABEL maintainer="milkrun-team"
LABEL description="The Milk-Run Backend - GraalVM Native Image"

WORKDIR /app
COPY --from=native-builder /app/milkrun-native .

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=2s --retries=3 \
    CMD curl -f http://localhost:8080/api/observability/live || exit 1

ENTRYPOINT ["./milkrun-native"]
