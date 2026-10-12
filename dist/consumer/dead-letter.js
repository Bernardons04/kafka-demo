import { kafka } from "../kafka.js";
const producer = kafka.producer();
export async function connectDeadLetterProducer() {
    await producer.connect();
}
export async function disconnectDeadLetterProducer() {
    await producer.disconnect();
}
export async function sendToDeadLetterQueue(topic, partition, offset, value, error) {
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
