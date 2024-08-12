import express from "express";
import { Kafka } from "kafkajs";

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: "kafka-demo",
  brokers: ['kafka:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'express-group' });

app.post('/send', async (req, res) => {
  const { message } = req.body;
  await producer.connect();
  await producer.send({
    topic: 'test-topic',
    messages: [{ value: message }],
  });
  await producer.disconnect();
  res.send('Message sent to Kafka');
});

app.get('/consume', async (req, res) => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

  const messages = [];
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Received message ${message.value} on topic ${topic} on the partition ${partition}`);
      messages.push(message.value.toString());
    },
  });

  setTimeout(async () => {
    await consumer.disconnect();
    res.json(messages);
  }, 1000);
});

app.listen(3000, () => console.log('Express server is running on port 3000'));