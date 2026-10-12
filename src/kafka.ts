import { Kafka } from "kafkajs";

export const kafka = new Kafka({
  clientId: "kafka-demo",
  brokers: ["localhost:9092"],
});