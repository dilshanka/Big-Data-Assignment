import { Kafka } from "kafkajs";
import pkg from "@kafkajs/confluent-schema-registry"; 
const { SchemaRegistry } = pkg;

const registry = new SchemaRegistry({ host: 'http://localhost:8081' });
const kafka = new Kafka({ clientId: 'order-consumer', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'order-group' });

const dlqProducer = kafka.producer();


let totalSum = 0.0;
let totalCount = 0;

const run = async () => {
    await consumer.connect();
    await dlqProducer.connect();
    
    await consumer.subscribe({ topic: 'orders', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                
                const decodedValue = await registry.decode(message.value);
                console.log(`\nReceived Order: ${decodedValue.orderId}`);

                
                if (decodedValue.price < 0) {
                    throw new Error("INVALID_PRICE: Price cannot be negative");
                }

                
                totalSum += decodedValue.price;
                totalCount++;
                const average = totalSum / totalCount;

                console.log(`   Product: ${decodedValue.product}`);
                console.log(`   Price: $${decodedValue.price}`);
                console.log(`   >>> NEW RUNNING AVERAGE: $${average.toFixed(2)}`);

            } catch (error) {
                
                
                if (error.message.includes("INVALID_PRICE")) {
                  
                    console.warn(`   [DLQ ALERT] Invalid message detected: ${error.message}`);
                    await sendToDLQ(message.value, error.message);
                } else {
                   
                    console.error("   [RETRY] System error. Retrying...");
                 
                }
            }
        },
    });
};

const sendToDLQ = async (originalMessage, errorReason) => {
    await dlqProducer.send({
        topic: 'orders-dlq',
        messages: [{ 
            value: originalMessage, // Send the raw bad data
            headers: { 'error-reason': errorReason } // Add context
        }]
    });
    console.log("   [DLQ] Message moved to 'orders-dlq' topic.");
};

run().catch(console.error);