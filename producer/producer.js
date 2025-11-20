import { Kafka } from "kafkajs";
import pkg from "@kafkajs/confluent-schema-registry";
const { SchemaRegistry, SchemaType } = pkg;
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);



const registry = new SchemaRegistry({ host: 'http://localhost:8081' });
const kafka = new Kafka({ clientId: 'order-producer', brokers: ['localhost:9092'] });
const producer = kafka.producer();

const run = async () => {

  const schemaPath = path.join(__dirname, '../schemas/order.avsc');
  const schema = fs.readFileSync(schemaPath, 'utf8');
  const { id } = await registry.register({ type: SchemaType.AVRO, schema });

  await producer.connect();
  console.log("Producer Connected");

  let counter = 1000;


  setInterval(async () => {
    counter++;
    const isBadMessage = Math.random() < 0.1;


    const value = {
      orderId: counter.toString(),
      product: ["Laptop", "Mouse", "Keyboard"][Math.floor(Math.random() * 3)],
      price: isBadMessage ? -100.0 : parseFloat((Math.random() * 100).toFixed(2))
    };

    try {

      const encodedValue = await registry.encode(id, value);

      await producer.send({
        topic: 'orders',
        messages: [{ value: encodedValue }]
      });

      console.log(`Sent: ${value.product} for $${value.price}`);
    } catch (e) {
      console.error("Error sending message", e);
    }
  }, 1000);
};

run().catch(console.error);