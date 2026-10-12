
import { kafka } from "../kafka.js";

const producer = kafka.producer();

export async function connectDeadLetterProducer(): Promise<void> {
  await producer.connect();
}

export async function disconnectDeadLetterProducer(): Promise<void> {
  await producer.disconnect();
}

export async function sendToDeadLetterQueue(
  topic: string,
  partition: number,
  offset: string,
  value: Buffer,
  error: unknown,
): Promise<void> {
  await producer.send({
    topic: `${topic}.DLQ`,
    messages: [
      {
        key: `${partition}-${offset}`,
        value: JSON.stringify({
          originalTopic: topic,
          originalPartition: partition,
          originalOffset: offset,
          originalValue: value.toString(),
          error: error instanceof Error ? error.message : String(error),
          failedAt: new Date().toISOString(),
        }),
      },
    ],
  });
}
