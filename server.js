import express from "express";
import { Kafka } from "kafkajs";
const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());

const kafka = new Kafka({
  clientId: "kafka-demo",
  brokers: ["kafka:9092"],
});

const producer = kafka.producer();
const admin = kafka.admin();
const consumer = kafka.consumer({ groupId: `kafka-group-${Date.now()}` });

const topics = ["demo"];

const initializeKafka = async () => {
  await producer.connect();
  await admin.connect();
  console.log("Kafka producer and admin initialized");
};

const ensureTopicExists = async (topic) => {
  try {
    const topicExists = await admin.fetchTopicMetadata({ topics: [topic] });
    if (!topicExists.topics.length) {
      await admin.createTopics({ topics: [{ topic }], });
      console.log(`Topic ${topic} created`);
    } else {
      console.log(`Topic ${topic} already exists`);
    }
  } catch (error) {
    console.error(`Error ensuring topic exists: ${error.message}`);
  }
};

const createConsumer = async (topic, cb) => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const msg = message.value.toString();
      console.log(`Received message ${msg} on topic ${topic}`);
      cb(msg);
    },
  });
  return consumer;
};

app.post("/send", async (req, res) => {
  const { topic, message } = req.body;
  try {
    await ensureTopicExists(topic);
    await producer.send({ topic, messages: [{ value: message }] });
    console.log(`Message sent to ${topic}: ${message}`);
    res.send(`Message sent to ${topic}`);
  } catch (error) {
    console.error("Error sending message:", error);
    res.status(500).send("Error sending message");
  }
});

const gracefulShutdown = async () => {
  console.log("Disconnecting Kafka producer, admin, and consumer...");
  await producer.disconnect();
  await admin.disconnect();
  await consumer.disconnect();
  process.exit();
};

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);

app.listen(PORT, async () => {
  console.log(`Express server is running on port ${PORT}`);
  await initializeKafka();
  await Promise.all(topics.map(ensureTopicExists));
  await createConsumer("demo", (msg) => {
    console.log(`Consumed message: ${msg}`);
  });
});
