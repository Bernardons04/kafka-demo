import { kafka } from "../kafka.js";
import { randomUUID } from "node:crypto";

const producer = kafka.producer();

await producer.connect();

console.log("Producer connected");

try {
  const result = await producer.send({
    topic: "payments",
    messages: [
      {
        key: "456",
        value: JSON.stringify({
          messageId: randomUUID(),
          amount: 5000,
        }),
      },
    ],
  });

  console.log("Message sent:", result);
} finally {
  await producer.disconnect();
}
