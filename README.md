# KafkaIntegration

Two Spring Boot services that talk through Apache Kafka, using the **Transactional Outbox pattern** so an order is never saved without its event (or vice versa).

```
                ┌──────────────────────── kafkaIntegrationProducer (:8081) ────────────────────────┐
 POST /orders → │ OrderController → OrderService ──@Transactional──► orders + outbox_events (PENDING) │
                │                                                         │                          │
                │                     Outboxpublisher (@Scheduled 5s) ◄───┘                          │
                │                          │  reads PENDING, sends, marks PUBLISHED                  │
                │                          ▼                                                         │
                │                    OrderKafkaProducer ──► topic "order-created" (key = orderId)    │
                └──────────────────────────────────────────────────┬───────────────────────────────┘
                                                                   ▼
                ┌──────────────── kafkaIntegrationConsumer (:8082) ──────────────────┐
                │ OrderKafkaConsumer (@KafkaListener, group "order-service")         │
                │   → markProcessed(orderId) in processed_orders; 0 → skip dup       │
                │   → prints value, partition, offset, key                           │
                └────────────────────────────────────────────────────────────────────┘
```

## Tech stack

| | Producer | Consumer |
|---|---|---|
| Spring Boot | 4.2.0-M2 | 4.1.1 |
| Java | 17 | 17 |
| Build | Gradle (wrapper included) | Gradle (wrapper included) |
| Kafka | spring-kafka | spring-kafka |
| DB | PostgreSQL + Spring Data JPA | PostgreSQL + Spring Data JPA (dedupe table) |
| Other | Lombok, Validation, Redis starter (not used yet), DevTools | Lombok, Validation, DevTools |

## Project layout

```
Kafka-Integration/
├── kafkaIntegrationProducer/                     # port 8081
│   ├── build.gradle
│   └── src/main/java/com/kgstrivers/kafkaIntegration/
│       ├── KafkaIntegrationApplication.java      # entry point, @EnableScheduling
│       ├── Configurations/JacksonConfig.java     # ObjectMapper bean
│       ├── Controllers/
│       │   ├── OrderController.java              # POST/GET /orders
│       │   └── LoadTestController.java           # POST /test/load
│       ├── Entities/
│       │   ├── Order.java                        # table "orders"
│       │   └── OutboxEvent.java                  # table "outbox_events"
│       ├── Events/OrderCreatedEvent.java         # Kafka message payload
│       ├── Kafka/OrderKafkaProducer.java         # KafkaTemplate wrapper
│       ├── Repositories/
│       │   ├── OrderRepository.java
│       │   └── OutboxEventRepository.java        # findByStatus(...)
│       └── Services/
│           ├── OrderService.java                 # saves order + outbox row atomically
│           ├── Outboxpublisher.java              # scheduled relay outbox → Kafka
│           └── LoadTestService.java              # 1M-request HTTP load generator
│
└── kafkaIntegrationConsumer/                     # port 8082
    ├── build.gradle
    └── src/main/java/com/kgstrivers/kafkaIntegrationConsumer/
        ├── KafkaIntegrationConsumerApplication.java
        ├── Entities/ProcessedOrder.java          # table "processed_orders" (dedupe)
        ├── Events/OrderCreatedEvent.java         # consumer's own copy of the payload
        ├── Kafka/OrderKafkaConsumer.java         # @KafkaListener on "order-created", dedupes
        └── Repositories/ProcessedOrderRepository.java  # markProcessed(orderId)
```

---

## Producer service (`kafkaIntegrationProducer`)

### `KafkaIntegrationApplication`
Spring Boot entry point. `@EnableScheduling` turns on the `@Scheduled` outbox relay.

### `Configurations/JacksonConfig`
Registers a plain `com.fasterxml.jackson.databind.ObjectMapper` bean, used by `OrderService` to serialize events into the outbox `payload` column.

### Entities

**`Order`** → table `orders`

| Field | Type | Notes |
|---|---|---|
| `id` | `Long` | PK, `IDENTITY` |
| `product` | `String` | |
| `amount` | `Double` | |

**`OutboxEvent`** → table `outbox_events`

| Field | Type | Notes |
|---|---|---|
| `id` | `Long` | PK, `IDENTITY` |
| `eventType` | `String` | e.g. `OrderCreated` |
| `aggregateId` | `Long` | the order id |
| `payload` | `String` (`TEXT`) | JSON of `OrderCreatedEvent` |
| `status` | `String` | `PENDING` → `PUBLISHED` |
| `createdAt` | `LocalDateTime` | |

Tables are created automatically (`spring.jpa.hibernate.ddl-auto=update`).

### `Events/OrderCreatedEvent`
Kafka message body: `orderId`, `product`, `amount`.

### Repositories
- `OrderRepository extends JpaRepository<Order, Long>`
- `OutboxEventRepository extends JpaRepository<OutboxEvent, Long>` with `findByStatus(String status)`.

### `Services/OrderService`
- `createOrder(Order)` — `@Transactional`. In one DB transaction:
  1. saves the `Order`,
  2. builds an `OrderCreatedEvent` from the saved order,
  3. saves an `OutboxEvent` with status `PENDING` and the event as JSON.

  Kafka is **not** called here — that's the point of the outbox: the order and its event commit or roll back together.
- `getAllOrders()` — returns all orders.

### `Services/Outboxpublisher`
`@Scheduled(fixedRate = 5000)` — every 5 s:
1. loads all `PENDING` outbox rows,
2. deserializes `payload` back into `OrderCreatedEvent`,
3. calls `OrderKafkaProducer.publishOrderCreated(...)`,
4. marks the row `PUBLISHED`.

If anything throws, the row stays `PENDING` and is retried on the next tick.

### `Kafka/OrderKafkaProducer`
Wraps `KafkaTemplate<String, OrderCreatedEvent>`. Sends to topic **`order-created`** with key = `orderId` (so all events for one order land on the same partition, preserving order). Logs partition/offset on ACK, or the exception on failure.

### Controllers

| Method | Path | Body | Description |
|---|---|---|---|
| `POST` | `/orders` | `{"product":"MacBook","amount":190000}` | Create order (+ outbox event) |
| `GET` | `/orders` | – | List all orders |
| `POST` | `/test/load` | – | Run the load test (blocks until done) |

### `Services/LoadTestService`
Fires **1,000,000** `POST /orders` requests at `http://localhost:8081/orders` using a fixed pool of **100** threads and Java's `HttpClient`. Counts successes (2xx) / failures with `AtomicInteger`, waits on a `CountDownLatch`, then prints total, success, failed, elapsed time and requests/sec.

### Producer configuration (`application.properties`)

| Property | Value |
|---|---|
| `server.port` | `8081` |
| `spring.datasource.url` | `jdbc:postgresql://localhost:5432/kafka_demo` |
| `spring.jpa.hibernate.ddl-auto` | `update` |
| `spring.jpa.show-sql` | `true` |
| `spring.kafka.bootstrap-servers` | `localhost:9092` |
| `spring.kafka.producer.key-serializer` | `StringSerializer` |
| `spring.kafka.producer.value-serializer` | `JsonSerializer` |
| `spring.kafka.producer.acks` | `1` (leader ack only) |
| `spring.kafka.producer.retries` | `3` |

---

## Consumer service (`kafkaIntegrationConsumer`)

### `KafkaIntegrationConsumerApplication`
Spring Boot entry point.

### `Events/OrderCreatedEvent`
The consumer's **own** copy of the event (`orderId`, `product`, `amount`) in its own package. The services share no code — only the JSON shape.

### `Kafka/OrderKafkaConsumer`
`@KafkaListener(topics = "order-created", groupId = "order-service")`, `@Transactional`. Receives `ConsumerRecord<String, OrderCreatedEvent>`, dedupes on `orderId`, then prints the value, partition, offset and key.

### Idempotency (dedupe on `orderId`)
- `Entities/ProcessedOrder` → table `processed_orders` (`order_id` PK, `processed_at`).
- `Repositories/ProcessedOrderRepository.markProcessed(orderId)` runs `INSERT ... ON CONFLICT DO NOTHING` and returns `1` (new) or `0` (already seen). One atomic statement, so it's race-safe even across rebalances.
- The listener calls it first; `0` → log and skip. The marker and the processing share one DB transaction, so a crash before commit rolls both back and the redelivered message is processed normally.
- Offsets are committed after the listener returns, so a crash after the DB commit but before the offset commit just causes a redelivery that gets skipped.

### Consumer configuration (`application.properties`)

| Property | Value | Why |
|---|---|---|
| `server.port` | `8082` | |
| `spring.jpa.hibernate.ddl-auto` | `update` | creates `processed_orders` |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | |
| `spring.kafka.consumer.auto-offset-reset` | `earliest` | read from the beginning for new groups |
| `key-deserializer` | `StringDeserializer` | |
| `value-deserializer` | `JsonDeserializer` | |
| `spring.json.use.type.headers` | `false` | ignore the producer's Java class name in headers (different package) |
| `spring.json.value.default.type` | `...kafkaIntegrationConsumer.Events.OrderCreatedEvent` | deserialize into the consumer's own class |
| `spring.json.trusted.packages` | `...kafkaIntegrationConsumer.Events` | |

> `spring.kafka.consumer.group-id=order-consumer-debug` is set in properties, but the `groupId = "order-service"` on `@KafkaListener` overrides it.

---

## Installation guide

You need three things running before the services start:

| Component | Version | Address | Used by |
|---|---|---|---|
| Java (JDK) | 17+ | – | both services |
| PostgreSQL | 14+ | `localhost:5432`, DB `kafka_demo`, user `postgres` / pw `1998` | both services |
| Apache Kafka | 3.7+ (KRaft, no ZooKeeper) | `localhost:9092` | both services |

Gradle does **not** need to be installed — each module ships with `./gradlew`.

Pick **Option A (Docker)** or **Option B (native, macOS/Homebrew)** for Postgres + Kafka.

### Step 1 — Install Java 17

macOS:

```bash
brew install openjdk@17
```

```bash
sudo ln -sfn $(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
```

Ubuntu/Debian:

```bash
sudo apt install -y openjdk-17-jdk
```

Windows: install [Eclipse Temurin 17](https://adoptium.net/temurin/releases/?version=17).

Verify:

```bash
java -version
```

### Step 2 — Clone the repo

```bash
git clone https://github.com/kaushikpuka1998/KafkaIntegration.git && cd KafkaIntegration
```

### Step 3 (Option A) — Postgres + Kafka with Docker

Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) and start it.

PostgreSQL (creates the `kafka_demo` DB automatically):

```bash
docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=1998 -e POSTGRES_DB=kafka_demo postgres:16
```

Kafka (single-node KRaft broker, auto-creates topics):

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:latest
```

Stop / start later:

```bash
docker stop kafka postgres
```

```bash
docker start postgres kafka
```

### Step 3 (Option B) — Postgres + Kafka natively (macOS)

PostgreSQL:

```bash
brew install postgresql@16 && brew services start postgresql@16
```

Homebrew creates a superuser named after your macOS user, not `postgres`. Create the role and DB the app expects:

```bash
psql postgres -c "CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD '1998';"
```

```bash
createdb -U postgres kafka_demo
```

Kafka (Homebrew runs it in KRaft mode on `localhost:9092`):

```bash
brew install kafka && brew services start kafka
```

### Step 4 — Create the topic (optional)

The broker auto-creates `order-created` on first send. To create it up front with 3 partitions (so the `orderId` key actually spreads load):

Docker:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic order-created --partitions 3 --replication-factor 1
```

Homebrew:

```bash
kafka-topics --bootstrap-server localhost:9092 --create --topic order-created --partitions 3 --replication-factor 1
```

### Step 5 — Verify infrastructure

Postgres reachable:

```bash
PGPASSWORD=1998 psql -h localhost -U postgres -d kafka_demo -c "select 1;"
```

Kafka reachable (Docker; drop the `docker exec kafka /opt/kafka/bin/` prefix and `.sh` for Homebrew):

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Step 6 — Different credentials?

If your Postgres user/password differ, override without editing files:

```bash
export SPRING_DATASOURCE_USERNAME=myuser SPRING_DATASOURCE_PASSWORD=mypass
```

### Troubleshooting

| Symptom | Fix |
|---|---|
| `Connection to localhost:5432 refused` | Postgres not running — `docker start postgres` / `brew services start postgresql@16` |
| `password authentication failed for user "postgres"` | Role/password mismatch — see Step 3B or Step 6 |
| `database "kafka_demo" does not exist` | `createdb -U postgres kafka_demo` |
| `Connection to node -1 (localhost/127.0.0.1:9092) could not be established` | Kafka not running — `docker start kafka` / `brew services start kafka` |
| `Port 8081/8082 was already in use` | `lsof -i :8081` and kill the process, or change `server.port` |
| `Unsupported class file major version` / toolchain error | `java -version` must be 17+; set `JAVA_HOME` accordingly |
| Consumer prints nothing | Wait ~5 s (outbox poll interval); check producer log for `Kafka ACK received` |

---

## Running locally

### 1. Make sure Postgres and Kafka are up
See the [Installation guide](#installation-guide).

### 2. Start the producer

```bash
cd kafkaIntegrationProducer && ./gradlew bootRun
```

### 3. Start the consumer

```bash
cd kafkaIntegrationConsumer && ./gradlew bootRun
```

### 4. Try it

```bash
curl -X POST localhost:8081/orders -H 'Content-Type: application/json' -d '{"product":"MacBook","amount":190000}'
```

```bash
curl localhost:8081/orders
```

Within ~5 s the producer logs `Kafka ACK received` with partition/offset, and the consumer prints:

```
Message = OrderCreatedEvent(orderId=1, product=MacBook, amount=190000.0), Partition = 0, Offset = 0, Key = 1
```

### 5. Load test (optional)

```bash
curl -X POST localhost:8081/test/load
```

This sends 1M requests and only returns when finished — results are printed in the producer log.

### Tests

```bash
./gradlew test
```

Each module has a `contextLoads()` smoke test (needs Postgres and Kafka running).

---

## Known caveats

- **Outbox marks `PUBLISHED` before Kafka ACKs.** `publishOrderCreated` is async; the row is saved as `PUBLISHED` right after `send()` is called, so a failed send is only logged, not retried. Fix: wait on the send future (`.get()`) or update status inside `whenComplete`.
- **At-least-once delivery.** If the app dies between sending and saving `PUBLISHED`, the event is re-sent. The consumer handles this by deduping on `orderId` (see *Idempotency*).
- **Two Jackson versions.** `OrderService` uses `com.fasterxml.jackson` (Jackson 2, from `JacksonConfig`); `Outboxpublisher` uses `tools.jackson` (Jackson 3, Boot 4 default).
- **Plaintext DB password** in `application.properties` — move to env vars (`SPRING_DATASOURCE_PASSWORD`) for anything beyond local dev.
- Redis starter (producer) is on the classpath but unused.
