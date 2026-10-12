import { kafka } from "../kafka.js";
import { processPayment } from "./process-payment.js";
import { paymentSchema } from "./payment-schema.js";
import { retry } from "./retry.js";
import { pool } from "../database/connection.js";
import { connectDeadLetterProducer, disconnectDeadLetterProducer, sendToDeadLetterQueue, } from "./dead-letter.js";
const consumer = kafka.consumer({
    groupId: "payments-group",
});
await consumer.connect();
await consumer.subscribe({
    topic: "payments",
    fromBeginning: true,
});
async function commitOffset(topic, partition, offset) {
    await consumer.commitOffsets([
        {
            topic,
            partition,
            offset: (BigInt(offset) + 1n).toString(),
        },
    ]);
}
await connectDeadLetterProducer();
console.log("Consumer subscribed");
await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) {
            console.error("Empty message value");
            await commitOffset(topic, partition, message.offset);
            return;
        }
        let payload;
        try {
            payload = JSON.parse(message.value.toString());
        }
        catch {
            console.error("Invalid JSON:", message.value.toString());
            await commitOffset(topic, partition, message.offset);
            return;
        }
        const result = paymentSchema.safeParse(payload);
        if (!result.success) {
            console.error("Invalid payment:", result.error.issues);
            await commitOffset(topic, partition, message.offset);
            return;
        }
        try {
            await retry(() => processPayment(result.data), {
                maxAttempts: 3,
                delayMs: 1000,
            });
        }
        catch (error) {
            console.error("Payment failed after retries:", message.offset);
            await sendToDeadLetterQueue(topic, partition, message.offset, message.value, error);
            console.log("Message sent to DLQ:", message.offset);
        }
        await commitOffset(topic, partition, message.offset);
        console.log({
            topic,
            partition,
            offset: message.offset,
        });
    },
});
async function shutdown() {
    console.log("Shutting down...");
    await consumer.stop();
    await consumer.disconnect();
    await disconnectDeadLetterProducer();
    await pool.end();
    console.log("Consumer stopped");
}
process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
