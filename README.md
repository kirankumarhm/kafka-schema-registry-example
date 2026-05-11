# Kafka Schema Registry Example

Production-grade Spring Boot application demonstrating Apache Kafka with Confluent Schema Registry using Avro serialization, manual offset commit, dead-letter topic error handling, and full observability.

## Architecture

```
                         ┌─────────────────┐
                         │  Schema Registry │
                         │  (Avro validation)│
                         └────────┬────────┘
                                  │
REST API → EventController → KafkaAvroProducer → [Kafka Broker (3 partitions)]
                                                          │
                                                   KafkaAvroConsumer
                                                          │
                                              ┌───────────┴───────────┐
                                              │                       │
                                        acknowledge()           nack() / retry
                                        (offset commit)               │
                                                              DLT (Dead Letter Topic)
```

## Tech Stack

- Java 21
- Spring Boot 4.0.6
- Apache Kafka with Confluent Schema Registry
- Apache Avro 1.12.1
- Confluent Kafka Avro Serializer 8.2.0
- Spring Boot Actuator + Micrometer Prometheus
- SpringDoc OpenAPI (Swagger UI)

## Prerequisites

- Java 21+
- Docker (for Kafka & Schema Registry)
- Maven 3.9+

## Running Infrastructure

Start Kafka, Zookeeper, and Schema Registry using Docker Compose:

```yaml
# docker-compose.yml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka-1:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  kafka-2:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9094:9094"
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9094
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  kafka-3:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9096:9096"
    environment:
      KAFKA_BROKER_ID: 3
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9096
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka-1:9092
```

```bash
docker-compose up -d
```

## Build & Run

### Local Development

```bash
mvn clean install
mvn spring-boot:run
```

### Production (Docker)

```bash
mvn clean package -DskipTests
docker build -t kafka-schema-registry-example .

docker run -p 8181:8181 \
  -e SPRING_PROFILES_ACTIVE=prod \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  -e KAFKA_CONSUMER_GROUP=my-consumer-group \
  kafka-schema-registry-example
```

Application runs on port **8181**.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_PROFILES_ACTIVE` | `default` | Active profile (`default`, `prod`) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9094,localhost:9096` | Kafka broker addresses |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry URL |
| `KAFKA_CONSUMER_GROUP` | `javatechie-new` | Consumer group ID |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:4200` | Allowed CORS origins |

## Profiles

| Profile | Swagger | Actuator Details | Log File | Banner | Use |
|---------|---------|-----------------|----------|--------|-----|
| `default` | ✅ Enabled | show-details: always | Console only | On | Local dev |
| `prod` | ❌ Disabled | show-details: never | `/var/log/app/*.log` | Off | Production |

## Avro Schema

Located at `src/main/resources/avro/employee.avsc`:

```json
{
  "namespace": "com.kafka.schema.registry.example.model",
  "type": "record",
  "name": "Employee",
  "fields": [
    { "name": "id", "type": "int" },
    { "name": "firstname", "type": "string" },
    { "name": "lastname", "type": "string" },
    { "name": "email", "type": ["null", "string"], "default": null },
    { "name": "age", "type": "int" },
    { "name": "dob", "type": "string" }
  ]
}
```

### Schema Field Rules

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| id | int | ✅ Yes | — | Must be an integer |
| firstname | string | ✅ Yes | — | Cannot be null |
| lastname | string | ✅ Yes | — | Cannot be null |
| email | null \| string | ❌ No | null | Can be null or omitted |
| age | int | ✅ Yes | — | Must be an integer |
| dob | string | ✅ Yes | — | Date as string (e.g. "01-05-1974") |

**Key Rule:** A field can be omitted from the request only if it has a `default` defined in the Avro schema. Fields without defaults are mandatory.

## API Examples

### POST /api/v1/events — Publish Employee Event

**Endpoint:** `POST http://localhost:8181/api/v1/events`

#### Example 1: All fields provided

```bash
curl -X POST http://localhost:8181/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1001,
    "firstname": "Kiran Kumar",
    "lastname": "HM",
    "email": "kiran@example.com",
    "age": 51,
    "dob": "01-05-1974"
  }'
```

**Response (202 Accepted):**
```
Message published - offset: 0
```

#### Example 2: Email set to null (valid — field is nullable)

```bash
curl -X POST http://localhost:8181/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1002,
    "firstname": "John",
    "lastname": "Doe",
    "email": null,
    "age": 30,
    "dob": "15-06-1994"
  }'
```

**Response (202 Accepted):**
```
Message published - offset: 1
```

#### Example 3: Email omitted (valid — has default value of null)

```bash
curl -X POST http://localhost:8181/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1003,
    "firstname": "Jane",
    "lastname": "Smith",
    "age": 28,
    "dob": "22-11-1996"
  }'
```

**Response (202 Accepted):**
```
Message published - offset: 2
```

#### Example 4: Missing required field (ERROR)

```bash
curl -X POST http://localhost:8181/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1004,
    "firstname": "Bob",
    "age": 40,
    "dob": "10-03-1984"
  }'
```

**Response (400 Bad Request):**
```json
{
  "timestamp": "2026-05-20T10:30:00",
  "status": 400,
  "error": "Bad Request",
  "message": "lastname is required",
  "path": "/api/v1/events"
}
```

#### Example 5: Kafka broker down (ERROR)

```bash
curl -X POST http://localhost:8181/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1005,
    "firstname": "Alice",
    "lastname": "Wonder",
    "age": 25,
    "dob": "10-10-1999"
  }'
```

**Response (500 Internal Server Error):**
```
Failed to publish message: Expiring 1 record(s)...
```

## Observability Endpoints

| URL | Purpose | Available In |
|-----|---------|-------------|
| http://localhost:8181/swagger-ui.html | Swagger UI (interactive API docs) | dev only |
| http://localhost:8181/api-docs | OpenAPI 3 JSON spec | dev only |
| http://localhost:8181/actuator/health | Health check (K8s readiness/liveness) | all |
| http://localhost:8181/actuator/health/kafka | Kafka broker connectivity | all |
| http://localhost:8181/actuator/info | Build info (version, timestamp) | all |
| http://localhost:8181/actuator/metrics | Application metrics | dev |
| http://localhost:8181/actuator/prometheus | Prometheus scrape endpoint | all |

### Health Check Response Example

```json
{
  "status": "UP",
  "components": {
    "kafka": {
      "status": "UP",
      "details": {
        "clusterId": "abc123"
      }
    }
  }
}
```

### Build Info Response Example (`/actuator/info`)

```json
{
  "build": {
    "artifact": "kafka-schema-registry-example",
    "name": "kafka-schema-registry-example",
    "version": "0.0.1-SNAPSHOT",
    "time": "2026-05-20T10:00:00Z"
  }
}
```

## Offset Commit Strategy

This application uses **manual offset commit** for reliable message processing:

| Setting | Value | Purpose |
|---------|-------|---------|
| `enable-auto-commit` | `false` | Prevents auto-committing before processing completes |
| `ack-mode` | `MANUAL_IMMEDIATE` | Offset committed immediately after `acknowledgment.acknowledge()` |
| `concurrency` | `3` | 3 concurrent consumer threads (matches partition count) |

### How It Works

1. Consumer receives message
2. Processes the message
3. Calls `acknowledgment.acknowledge()` → offset committed
4. If processing fails → `acknowledgment.nack(Duration.ofSeconds(1))` → message retried after 1s

### Error Handling Flow

```
Message received → Process → Success? → acknowledge() → offset committed
                                ↓ No
                         Retry (3 times, 2s interval)
                                ↓ Still failing
                         Publish to DLT (javatechie-avro.DLT)
```

### Non-Retryable Exceptions (sent directly to DLT)

- `SerializationException` — corrupted message, retry won't help
- `RecordDeserializationException` — schema mismatch
- `NullPointerException` — programming error, not transient

## Producer Configuration

| Setting | Value | Purpose |
|---------|-------|---------|
| `acks` | `all` | Wait for all replicas to acknowledge |
| `retries` | `3` | Retry on transient failures |
| `enable.idempotence` | `true` | Exactly-once producer semantics |
| `max.in.flight.requests.per.connection` | `5` | Max unacknowledged requests (safe with idempotence) |
| `delivery.timeout.ms` | `60000` | Total time to deliver a message |
| `request.timeout.ms` | `30000` | Time to wait for broker response |

## Message Ordering Guarantee

Kafka guarantees message ordering **only within a single partition**, not across partitions.

### How Partitioning Works

```
Producer sends with key → hash(key) % numPartitions = target partition
```

Messages with the **same key** always go to the **same partition** → ordered.
Messages with **different keys** may go to different partitions → no ordering between them.

### Our Approach: Business Key Partitioning

We use `employee.id` as the Kafka message key:

```java
String key = String.valueOf(employee.getId());
kafkaTemplate.send(topicName, key, employee);
```

This ensures all events for the same employee land on the same partition:

```
Employee 1001 → hash("1001") % 3 = partition 2 (always)
Employee 1002 → hash("1002") % 3 = partition 0 (always)
Employee 1003 → hash("1003") % 3 = partition 1 (always)
```

### Why NOT Random UUID Keys?

```java
// ❌ BAD - no ordering guarantee for same employee
kafkaTemplate.send(topicName, UUID.randomUUID().toString(), employee);

// ✅ GOOD - all events for employee 1001 are ordered
kafkaTemplate.send(topicName, String.valueOf(employee.getId()), employee);
```

### Ordering Strategies Comparison

| Strategy | Key | Ordering | Throughput | Use Case |
|----------|-----|----------|------------|----------|
| Random UUID | `UUID.randomUUID()` | ❌ None | High | Fire-and-forget events |
| Business ID | `employee.getId()` | ✅ Per entity | High | Event sourcing, state changes |
| Single partition | Any key, 1 partition | ✅ Global (total) | Low | Strict total ordering required |
| Null key | `null` | ❌ Round-robin | High | Load balancing, no ordering needed |

### Important Considerations

- **Concurrency vs Ordering**: With `concurrency: 3`, each consumer thread handles one partition. Ordering is preserved per partition.
- **Partition count changes**: If you increase partitions, existing key-to-partition mappings change. Plan partition count upfront.
- **Hot partitions**: If one employee ID has significantly more events, its partition becomes a hotspot. Use composite keys if needed (e.g., `employeeId + eventType`).

## Schema Evolution

Avro + Schema Registry supports backward/forward compatible schema changes:

| Change | Compatible? | Notes |
|--------|-------------|-------|
| Add field with default | ✅ Backward | Old consumers ignore new field |
| Remove field with default | ✅ Forward | New consumers use default |
| Rename field | ❌ Breaking | Use aliases instead |
| Change type | ❌ Breaking | Unless promotable (int → long) |

Example — adding a new optional field:
```json
{ "name": "department", "type": ["null", "string"], "default": null }
```

## Schema Registry Endpoints

```bash
# List all subjects
curl http://localhost:8081/subjects

# Get schema for topic
curl http://localhost:8081/subjects/javatechie-avro-value/versions/latest

# Check compatibility
curl -X POST http://localhost:8081/compatibility/subjects/javatechie-avro-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"Employee\",\"namespace\":\"com.kafka.schema.registry.example.model\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"firstname\",\"type\":\"string\"},{\"name\":\"lastname\",\"type\":\"string\"},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"dob\",\"type\":\"string\"}]}"}'
```

## Kafka CLI Commands

```bash
# List topics
kafka-topics --bootstrap-server localhost:9092 --list

# Describe consumer group offsets
kafka-consumer-groups --bootstrap-server localhost:9092 --group javatechie-new --describe

# Read from DLT
kafka-console-consumer --bootstrap-server localhost:9092 --topic javatechie-avro.DLT --from-beginning

# Reset offsets (if needed)
kafka-consumer-groups --bootstrap-server localhost:9092 --group javatechie-new --topic javatechie-avro --reset-offsets --to-earliest --execute
```

## Docker

### Dockerfile

```dockerfile
FROM eclipse-temurin:21-jre-alpine AS runtime
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
WORKDIR /app
COPY target/kafka-schema-registry-example-*.jar app.jar
RUN chown -R appuser:appgroup /app
USER appuser
EXPOSE 8181
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:8181/actuator/health || exit 1
ENTRYPOINT ["java", "-XX:+UseG1GC", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
```

### Docker Build & Run

```bash
# Build
mvn clean package -DskipTests
docker build -t kafka-schema-registry-example .

# Run (dev)
docker run -p 8181:8181 kafka-schema-registry-example

# Run (prod)
docker run -p 8181:8181 \
  -e SPRING_PROFILES_ACTIVE=prod \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  kafka-schema-registry-example
```

## Project Structure

```
src/main/java/com/kafka/schema/registry/example/
├── config/
│   ├── ApiErrorResponse.java          # Standardized error response DTO
│   ├── GlobalExceptionHandler.java    # Global exception handler
│   ├── KafkaConfig.java               # Kafka consumer factory, DLT, error handler
│   ├── OpenApiConfig.java             # Swagger/OpenAPI configuration
│   ├── RequestLoggingFilter.java      # HTTP request/response logging
│   └── WebConfig.java                 # CORS configuration
├── consumer/
│   └── KafkaAvroConsumer.java         # Kafka consumer with manual ack
├── controller/
│   └── EventController.java           # REST API with async response
├── model/
│   └── Employee.java                  # Avro-generated model
├── producer/
│   └── KafkaAvroProducer.java         # Kafka producer with idempotency
└── KafkaSchemaRegistryExampleApplication.java

src/main/resources/
├── avro/
│   └── employee.avsc                  # Avro schema definition
├── application.yaml                   # Default configuration
└── application-prod.yaml              # Production overrides
```

## Production Checklist Coverage

Based on [Spring Boot Production Checklist](https://medium.com/lets-code-future/spring-boot-production-checklist-the-67-things-i-wish-i-checked-before-that-120k-deployment-686b8b8ceb2c):

| # | Category | Item | Status |
|---|----------|------|--------|
| 1 | Config | Externalized with env vars | ✅ |
| 2 | Config | Multi-profile (default/prod) | ✅ |
| 3 | Config | Secrets not hardcoded | ✅ |
| 4 | Config | Banner disabled in prod | ✅ |
| 5 | Config | Graceful shutdown | ✅ |
| 6 | Config | Lifecycle timeout (30s) | ✅ |
| 7 | Config | Producer timeouts configured | ✅ |
| 8 | Logging | SLF4J (no System.out) | ✅ |
| 9 | Logging | Parameterized logging | ✅ |
| 10 | Logging | Log levels configured | ✅ |
| 11 | Logging | Structured log pattern | ✅ |
| 12 | Logging | File logging in prod | ✅ |
| 13 | Logging | Request/response logging filter | ✅ |
| 14 | Logging | No PII in logs | ✅ |
| 15 | Error | Global exception handler | ✅ |
| 16 | Error | Standardized error response DTO | ✅ |
| 17 | Error | Proper HTTP status codes | ✅ |
| 18 | Error | No stack traces leaked to client | ✅ |
| 19 | Health | Actuator enabled | ✅ |
| 20 | Health | Kafka health indicator | ✅ |
| 21 | Health | Readiness/Liveness probes | ✅ |
| 22 | Health | Build info endpoint | ✅ |
| 23 | Metrics | Micrometer + Prometheus | ✅ |
| 24 | API | REST versioning (/api/v1) | ✅ |
| 25 | API | OpenAPI/Swagger docs | ✅ |
| 26 | API | Swagger disabled in prod | ✅ |
| 27 | API | CORS configured | ✅ |
| 28 | API | Async response handling | ✅ |
| 29 | Kafka | Idempotent producer | ✅ |
| 30 | Kafka | acks=all | ✅ |
| 31 | Kafka | Manual offset commit | ✅ |
| 32 | Kafka | Dead letter topic | ✅ |
| 33 | Kafka | Retry with backoff | ✅ |
| 34 | Kafka | Non-retryable exceptions | ✅ |
| 35 | Kafka | Business key partitioning | ✅ |
| 36 | Kafka | Concurrency matches partitions | ✅ |
| 37 | Code | Constructor injection | ✅ |
| 38 | Code | No field injection | ✅ |
| 39 | Code | Clean package structure | ✅ |
| 40 | Code | Separation of concerns | ✅ |
| 41 | Build | Lombok excluded from jar | ✅ |
| 42 | Build | Build-info generated | ✅ |
| 43 | Build | Maven wrapper included | ✅ |
| 44 | Docker | Non-root user | ✅ |
| 45 | Docker | JVM tuning (G1GC, RAM%) | ✅ |
| 46 | Docker | Health check in Dockerfile | ✅ |
| 47 | Docker | .dockerignore | ✅ |
| 48 | Docker | Minimal base image (alpine) | ✅ |
| 49 | Docs | README with examples | ✅ |
| 50 | Docs | API documented (Swagger) | ✅ |
| 51 | Security | Actuator restricted in prod | ✅ |
| 52 | Security | No sensitive data in responses | ✅ |

### Not Included (infrastructure/team-level decisions)

| Item | Reason |
|------|--------|
| JWT/OAuth2 authentication | Depends on auth provider (Cognito, Keycloak) |
| Distributed tracing (Zipkin/Jaeger) | Add when multiple microservices exist |
| CI/CD pipeline | Infra-level (Jenkins, GitHub Actions, GitLab CI) |
| K8s manifests | Depends on orchestration platform |
| Rate limiting | Add if publicly exposed (use Bucket4j or API Gateway) |
| Integration tests | Add per team testing strategy |

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `null` value rejected | Field type is `"string"` not `["null","string"]` | Use union type with null |
| Missing field error | No `default` in schema | Add default or include field in request |
| Schema not found | Registry not running | Start schema-registry container |
| Offset not committed | Auto-commit disabled but no `acknowledge()` | Call `acknowledgment.acknowledge()` |
| Messages re-processed | Consumer restarted before commit | Expected with at-least-once; make processing idempotent |
| Messages out of order | Random/null key used | Use deterministic business key (e.g., entity ID) |
| Hot partition | Skewed key distribution | Use composite key or increase partitions |
| PKIX path building failed | SSL cert not trusted by JVM | Import cert into JVM truststore |
| Cached resolution failure | Maven cached failed download | Run `mvn clean install -U` or delete `~/.m2/repository/io/confluent` |
| 500 on publish | Kafka broker unreachable | Check `KAFKA_BOOTSTRAP_SERVERS` and broker health |
| Swagger UI not loading | Wrong profile or path | Verify `SPRING_PROFILES_ACTIVE` is not `prod` |
