# Kafka Demo — Asynchronous Payment Processing

A backend project demonstrating asynchronous message processing with Apache Kafka, Node.js, TypeScript, and PostgreSQL.

The application publishes payment messages to Kafka, processes them asynchronously, and persists the results in PostgreSQL. It also demonstrates retry handling, Dead Letter Queue (DLQ), manual offset commits, message validation, and idempotent database operations.

## Architecture

```text
Producer (Node.js / TypeScript)
             |
             v
       Kafka: payments
       (4 partitions)
             |
             v
       Consumer Group
       payments-group
             |
             v
       Message Validation
            (Zod)
             |
             v
       Payment Processing
             |
        +----+----+
        |         |
      Success   Failure
        |         |
        v         v
    PostgreSQL  Retry (up to 3 attempts)
                  |
            Retries exhausted
                  |
                  v
            Kafka: payments.DLQ
```

## Tech Stack

- **Node.js + TypeScript** — application runtime and type safety.
- **Apache Kafka** — asynchronous messaging and event streaming.
- **KafkaJS** — Kafka client for Node.js.
- **PostgreSQL** — persistent storage for processed payments.
- **Zod** — message payload validation.
- **Docker Compose** — local Kafka and PostgreSQL infrastructure.

## Key Concepts Demonstrated

- **Producers and consumers:** publishing and consuming payment messages.
- **Topics and partitions:** organizing messages and distributing them across Kafka partitions.
- **Consumer groups:** coordinating consumers under a shared group ID.
- **Manual offset commits:** committing offsets only after successful processing or successful DLQ publication.
- **Retry handling:** retrying failed payment processing up to three attempts in total.
- **Dead Letter Queue:** publishing messages to `payments.DLQ` after processing retries are exhausted.
- **Idempotency:** using a unique `message_id` constraint and `ON CONFLICT DO NOTHING` to prevent duplicate database inserts.
- **Message validation:** parsing JSON and validating payment payloads with Zod.
- **Resource management:** connecting and disconnecting Kafka producers and consumers, and closing the database connection during shutdown.

## Prerequisites

Make sure you have installed:

- Node.js and npm
- Docker Desktop with Docker Compose
- Git

## Getting Started

### 1. Install dependencies

From the project root, run:

```bash
npm install
```

### 2. Start Kafka and PostgreSQL

```bash
docker compose up -d
```

Verify that both containers are running:

```bash
docker compose ps
```

### 3. Initialize the database

Execute the SQL schema to create the `payments` table:

```bash
docker exec -i kafka-demo-postgres psql -U kafka-demo -d kafka-demo < src/database/schema.sql
```

Run this command when initializing a fresh database. The PostgreSQL Docker volume persists data between container restarts.

### 4. Create Kafka topics

The application uses two topics:

- `payments` — incoming payment messages.
- `payments.DLQ` — messages that failed processing after all retry attempts.

The broker is configured with four default partitions for automatically created topics. If automatic topic creation is disabled or the topics do not exist, create them explicitly.

Create the `payments` topic:

```bash
docker exec kafka-demo-broker /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic payments \
  --bootstrap-server localhost:9092 \
  --partitions 4 \
  --replication-factor 1
```

Create the `payments.DLQ` topic:

```bash
docker exec kafka-demo-broker /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic payments.DLQ \
  --bootstrap-server localhost:9092 \
  --partitions 4 \
  --replication-factor 1
```

On Git Bash for Windows, prefix these commands with `MSYS_NO_PATHCONV=1` if path conversion causes issues:

```bash
MSYS_NO_PATHCONV=1 docker exec ...
```

If a topic already exists, you do not need to recreate it.

### 5. Run the consumer

In one terminal:

```bash
npm run consumer
```

The consumer subscribes to `payments`, validates incoming messages, processes valid payments, and manually commits offsets according to the processing outcome.

### 6. Run the producer

In a second terminal:

```bash
npm run producer
```

The producer publishes a payment containing a unique `messageId` and an `amount`.

Check the consumer terminal for the processing result.

### 7. Verify persisted payments

Connect to PostgreSQL:

```bash
docker exec -it kafka-demo-postgres psql -U kafka-demo -d kafka-demo
```

Run the following query:

```sql
SELECT id, message_id, amount, created_at, processed_at
FROM payments
ORDER BY created_at DESC
LIMIT 10;
```

Exit the PostgreSQL shell with:

```sql
\q
```

## Testing Retry and DLQ

To verify the failure-handling flow during local development:

1. Temporarily introduce a simulated error in the payment processing path.
2. Start the consumer and publish a payment.
3. Verify that processing is attempted up to three times in total.
4. Verify that the failed message is published to `payments.DLQ`.
5. Verify that the original message offset is committed only after the DLQ publication succeeds.
6. Remove the simulated error and confirm that normal payment processing works again.

To inspect DLQ messages from Git Bash on Windows:

```bash
MSYS_NO_PATHCONV=1 docker exec kafka-demo-broker \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic payments.DLQ \
  --from-beginning \
  --max-messages 1
```

The DLQ payload includes the original topic, partition, offset, message value, error, and failure timestamp.

## Message Validation

Incoming messages are parsed as JSON and validated against a Zod schema.

Messages containing invalid JSON or payloads that fail schema validation are logged and discarded, with their offsets committed. They are not sent to the DLQ in the current implementation.

The DLQ is reserved for messages whose payment processing fails after all retry attempts.

## Idempotency and Delivery Guarantees

The `payments` table enforces uniqueness on `message_id`.

When the same payment message is processed more than once, PostgreSQL ignores the duplicate insert using `ON CONFLICT DO NOTHING`. This prevents duplicate database records for the same message ID.

The project does not guarantee exactly-once processing across Kafka and PostgreSQL. If the application crashes after publishing a failed message to the DLQ but before committing the original offset, the original message may be consumed again, resulting in duplicate DLQ messages.

## Project Structure

```text
src/
├── consumer/
│   ├── consumer.ts
│   ├── dead-letter.ts
│   ├── payment-schema.ts
│   ├── process-payment.ts
│   └── retry.ts
├── producer/
│   └── producer.ts
├── database/
│   ├── connection.ts
│   ├── payment.repository.ts
│   └── schema.sql
└── kafka.ts
```

## Stopping the Environment

Stop the containers:

```bash
docker compose down
```

To also remove the persisted PostgreSQL volume and its data:

```bash
docker compose down -v
```

**Warning:** the second command permanently removes the project's persisted database volume and its data.
