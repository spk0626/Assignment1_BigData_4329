# Orders Producer and Streams Application

This module provides a Kafka producer that sends order messages with retry and dead-letter queue (DLQ) support, and a Kafka Streams application that computes running averages per product.

## Features

### ProducerApp
- **Retry Logic**: Configurable retry mechanism for transient failures (network, broker issues).
  - Uses exponential backoff (1000ms default).
  - Retries up to 3 times before failing.
- **Producer Config**: Configured with strong durability settings:
  - `acks=all` (wait for all replicas)
  - `retries=3`
  - `enable.idempotence=true` (exactly-once semantics)
- **Dead Letter Queue (DLQ)**: Failed messages are written to `orders-dlq` topic as JSON strings for later inspection/replay.

### StreamsApp
- **Running Averages**: Reads Avro orders from `orders` topic, groups by product, and computes running average price.
- **Output**: Writes JSON formatted averages to `orders-averages` topic in format: `{"average":XX.XX,"count":N}`
- **Aggregate State**: Uses a simple aggregate (sum and count) with JSON serialization for the state store.

## Requirements

- Java 17+
- Maven 3.6+
- Docker (for Kafka, Zookeeper, Schema Registry)
- Docker Compose

## Quick Start

```bash
# 1. Start Kafka infrastructure
docker-compose up -d

# 2. Build the project
cd producer
mvn clean package

# 3. Run the producer (in one terminal)
mvn exec:java "-Dexec.mainClass=com.example.producer.ProducerApp"

# 4. Run the streams app (in another terminal)
mvn exec:java "-Dexec.mainClass=com.example.streams.StreamsApp"

# 5. View results
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders-averages --from-beginning --timeout-ms 10000
```

## Setup

### 1. Start Kafka infrastructure
From the root directory:
```bash
docker-compose up -d
```

This starts:
- Zookeeper (port 22181)
- Kafka (ports 9092, 9093)
- Confluent Schema Registry (port 8081)

### 2. Create required topics (optional; will auto-create)
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic orders --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic orders-dlq --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic orders-averages --partitions 1 --replication-factor 1
```

### 3. Build the producer module
```bash
cd producer
mvn clean package
```

## Running

### Run the Producer (sends 50 orders)
```bash
cd producer
mvn exec:java "-Dexec.mainClass=com.example.producer.ProducerApp"
```

Expected output: 50 lines of `Sent order=<UUID> key=<Item> to partition=0 offset=N`

### Run the Streams App (computes and writes averages)
In a separate terminal:
```bash
cd producer
mvn exec:java "-Dexec.mainClass=com.example.streams.StreamsApp"
```

The Streams app will start processing messages from `orders` and output running averages to `orders-averages`.

## Verification

### View messages on orders-averages topic
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders-averages --from-beginning --timeout-ms 10000
```

Example output:
```
Item1	{"average":45.67,"count":5}
Item2	{"average":32.10,"count":3}
Item3	{"average":78.95,"count":2}
```

### View DLQ messages (if any failures occurred)
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders-dlq --from-beginning --timeout-ms 10000
```

### Check all topics
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## Code Structure

```
producer/
├── pom.xml                                    # Maven config with Kafka and Jackson deps
├── README.md                                  # This file
└── src/main/java/
    ├── com/example/producer/
    │   └── ProducerApp.java                   # Producer with retry + DLQ
    ├── com/example/streams/
    │   └── StreamsApp.java                    # Streams aggregation app
    └── com/example/serde/
        ├── JsonSerializer.java                # Generic JSON serializer
        └── JsonDeserializer.java              # Generic JSON deserializer
```

## Architecture

### ProducerApp Flow
1. Create Order objects (random product, price).
2. Wrap `producer.send()` in `sendWithRetry()`.
3. On success: log offset and partition.
4. On retriable error (network, broker down): retry with backoff.
5. On non-retriable error or exhausted retries: write to `orders-dlq` as JSON string.

### StreamsApp Flow
1. Read Avro orders from `orders` topic using KafkaAvroDeserializer.
2. Re-key by product name (`v.getProduct()`).
3. Aggregate using KTable with sum and count.
4. Map aggregate to JSON format: `{"average":XX.XX,"count":N}`.
5. Write to `orders-averages` topic as string values.

## Dependencies

- Kafka Clients 3.6.0
- Kafka Streams 3.6.0
- Confluent Avro Serializer 7.4.0
- Apache Avro 1.11.1
- Jackson Databind 2.14.2
- SLF4J Simple 2.0.7

## Notes

- **Avro Schema**: Order schema is in `../schemas/order.avsc`. Avro classes are generated at compile time.
- **Schema Registry**: Streams app requires Schema Registry at `http://localhost:8081`.
- **State Store**: Streams app stores intermediate aggregates in local state (can be configured for changelogging).
- **Exactly-Once**: Producer uses `enable.idempotence=true` for exactly-once semantics within the producer timeout window.

## Troubleshooting

### Producer times out
Ensure Kafka broker is running and reachable at `localhost:9092`.

### Streams app won't start
- Check Schema Registry is up on `localhost:8081`.
- Ensure `orders` topic exists or allow auto-creation.

### No messages in orders-averages
- Verify `orders` topic has messages (run producer first).
- Check Streams app is still running (should not exit unless killed).

## Future Improvements

- Add metrics/monitoring (Micrometer, Prometheus).
- Improve DLQ with Avro schema for better structure.
- Add exponential backoff with jitter to retry logic.
- Use typed Avro aggregate instead of JSON for state store.
- Add integration tests.

---

For more information on Kafka Streams, see: https://kafka.apache.org/documentation/streams/
