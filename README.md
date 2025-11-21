
# Kafka Order Processing System

A simple Kafka-based order processing system with Avro serialization, real-time aggregation, and dead letter queue handling.

## Setup

**Install dependencies:**
```bash
npm install
Start Kafka cluster:

Bash
#Start Kafka and Schema Registry with Docker
docker-compose up -d



Bash

# Generates and sends orders (infinite stream)
npm run start-producer
Run consumer:

Bash

# Processes orders and calculates running average
npm run start-consumer
Run DLQ reprocessor:

Bash

# Fixes and resubmits failed messages
npm run start-reprocessor
Services
Kafka Broker: localhost:9092

Schema Registry: localhost:8081

Components
producer/producer.js - Generates and sends orders to Kafka

consumer/consumer.js - Processes orders with retry logic & aggregation

dlq_reprocess/reprocess.js - Handles and fixes failed messages

schemas/order.avsc - Avro schema definition