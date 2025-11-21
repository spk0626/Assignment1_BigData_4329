# Apache Kafka Orders Producer & Streams

A Apache Kafka application featuring a producer with retry logic and dead-letter queue (DLQ) support, along with a Kafka Streams application.

## Features

### Producer
- **Retries on Failure**: Automatically retries up to 3 times with 1-second intervals.  
- **Dead Letter Queue (DLQ)**: Failed messages go to `orders-dlq` for review.  
- **Safe Delivery**: Ensures no duplicates with `acks=all`, `retries=3`, and `enable.idempotence=true`.  

### Streams
- **Average Price per Product**: Computes running averages using Kafka Streams.  
- **JSON Output**: Sends results to `orders-averages` topic.  
- **Stateful**: Keeps track of aggregates using KTable.  


## Technology Stack

- **Apache Kafka 3.6.0**: Distributed event streaming platform
- **Kafka Streams 3.6.0**: Stream processing library
- **Confluent Schema Registry 7.4.0**: Schema management with Avro
- **Apache Avro 1.11.1**: Data serialization format
- **Jackson Databind 2.14.2**: JSON processing
- **Java 17**: Programming language
- **Maven 3.6+**: Build tool
- **Docker & Docker Compose**: Container orchestration

## Quick Start

### 1. Start Kafka Infrastructure
```bash
docker-compose up -d
```

### 2. Build the Project
```bash
cd producer
mvn clean package
```

### 3. Run the Producer
```bash
mvn exec:java "-Dexec.mainClass=com.example.producer.ProducerApp"
```

### 4. Run the Streams Application (in another terminal)
```bash
mvn exec:java "-Dexec.mainClass=com.example.streams.StreamsApp"
```

### 5. View Results
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders-averages --from-beginning --timeout-ms 10000
```

## Project Structure

```
.
├── producer/                          # Producer module
│   ├── pom.xml
│   ├── README.md                      # Detailed producer documentation
│   └── src/main/java/com/example/
│       ├── producer/
│       │   └── ProducerApp.java       # Producer with retry + DLQ
│       ├── streams/
│       │   └── StreamsApp.java        # Streams aggregation app
│       └── serde/
│           ├── JsonSerializer.java
│           └── JsonDeserializer.java
├── consumer/                          # Consumer module
│   ├── pom.xml
│   └── src/main/java/com/example/consumer/
│       └── ConsumerApp.java
├── streams/                           # Streams module
│   ├── pom.xml
│   └── src/main/java/com/example/streams/
│       └── StreamsAverage.java
├── schemas/
│   └── order.avsc                     # Avro schema for Order
├── docker-compose.yml                 # Docker services configuration
└── README.md                          # This file
```

## Architecture

### Message Flow
1. **Producer** generates order messages with random products and prices
2. Orders are sent to the `orders` Kafka topic with retry logic
3. Failed messages are routed to the `orders-dlq` topic
4. **Streams App** reads from `orders` topic and computes running averages
5. Aggregated results are written to `orders-averages` topic

### Retry Strategy
- **Transient Errors** (network, broker temporarily down): Automatic retry with exponential backoff
- **Non-Retriable Errors** (serialization, schema issues): Immediate routing to DLQ
- **Max Retries**: After 3 failed attempts, message is sent to DLQ


## Requirements

- Java 17 or higher
- Maven 3.6 or higher
- Docker and Docker Compose
- At least 4GB RAM available for Docker containers

