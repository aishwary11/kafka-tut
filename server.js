import express from "express";
import { Kafka, logLevel } from "kafkajs";

const app = express();
app.use(express.json());

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  clientId: "kafka-demo",
  brokers: ["kafka:9092"],
});

const producer = kafka.producer();
const admin = kafka.admin();
const consumers = {}; // Store consumers for different topics
const messages = {}; // Store messages for different topics

const topics = ["demo"]; // Define your topics here

// Function to initialize the Kafka producer
const initializeKafka = async () => {
  await producer.connect();
  console.log("Kafka producer initialized");
};

// Function to ensure topic exists
const ensureTopicExists = async (topic) => {
  await admin.connect();
  const topics = await admin.listTopics();
  if (!topics.includes(topic)) {
    await admin.createTopics({
      topics: [{ topic }],
    });
    console.log(`Topic ${topic} created`);
  } else {
    console.log(`Topic ${topic} already exists`);
  }
  await admin.disconnect();
};

// Function to create and run a consumer for a specific topic
const createConsumer = async (topic) => {
  if (consumers[topic]) {
    return consumers[topic];
  }
  const consumer = kafka.consumer({ groupId: `express-group-${topic}-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });
  messages[topic] = [];
  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Received message ${message.value.toString()} on topic ${topic}`);
      messages[topic].push(message.value.toString());
    },
  });
  consumers[topic] = consumer; // Store the consumer for later use
  console.log(`Consumer created and running for topic ${topic}`);
  return consumer;
};

// Function to initialize consumers for predefined topics
const initializeConsumers = async () => {
  for (const topic of topics) {
    await ensureTopicExists(topic); // Ensure the topic exists
    await createConsumer(topic);
  }
};

// Route to send a message to Kafka
app.post("/send", async (req, res) => {
  const { topic, message } = req.body;
  try {
    await ensureTopicExists(topic); // Ensure the topic exists
    await producer.send({
      topic,
      messages: [{ value: message }],
    });
    console.log(`Message sent to ${topic}: ${message}`);
    res.send(`Message sent to ${topic}`);
  } catch (error) {
    console.error("Error sending message:", error);
    res.status(500).send("Error sending message");
  }
});

// Route to get consumed messages for a specific topic
app.get("/consume", async (req, res) => {
  const { topic } = req.query;
  try {
    if (!messages[topic]) {
      return res.status(404).send("No messages found for this topic.");
    }
    const consumedMessages = [...messages[topic]]; // Clone messages using spread operator
    messages[topic].length = 0; // Clear messages after cloning
    res.json(consumedMessages);
  } catch (error) {
    console.error("Error consuming messages:", error);
    res.status(500).send("Error consuming messages");
  }
});

// Start the Express server and initialize Kafka
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`Express server is running on port ${PORT}`);
  await initializeKafka();
  await initializeConsumers(); // Initialize consumers for predefined topics
});

// Handle graceful shutdown
const gracefulShutdown = async () => {
  console.log("Disconnecting Kafka producer and consumers...");
  await producer.disconnect();
  for (const consumer of Object.values(consumers)) {
    await consumer.disconnect();
  }
  process.exit();
};

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);
