
import { Kafka } from "kafkajs";
const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry');
const path = require('path');
const fs = require('fs');


const registry = new SchemaRegistry({ host: 'http://localhost:8081' });
const kafka = new Kafka({ clientId: 'dlq-reprocessor', brokers: ['localhost:9092'] });


const consumer = kafka.consumer({ groupId: 'dlq-fixer-group' });
const producer = kafka.producer();

const run = async () => {

    await consumer.connect();
    await producer.connect();
    

    await consumer.subscribe({ topic: 'orders-dlq', fromBeginning: true });
    console.log(" Reprocessor started. Listening for failed messages...");

    const schemaPath = path.join(__dirname, '../schemas/order.avsc');
    const schema = fs.readFileSync(schemaPath, 'utf8');
    const { id } = await registry.register({ type: SchemaType.AVRO, schema });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
         
                const decodedValue = await registry.decode(message.value);
                const errorReason = message.headers['error-reason'].toString();
                
                console.log(`\n[DLQ RECEIVED] Order: ${decodedValue.orderId}`);
                console.log(`   Reason: ${errorReason}`);
                console.log(`  Original Data: Product=${decodedValue.product}, Price=${decodedValue.price}`);

                
                const fixedPrice = Math.abs(decodedValue.price);
                
                const fixedOrder = {
                    ...decodedValue,
                    price: fixedPrice
                };

                console.log(` Fixing: Converted price from $${decodedValue.price} to $${fixedOrder.price}`);

      
                const encodedValue = await registry.encode(id, fixedOrder);
                
                await producer.send({
                    topic: 'orders', 
                    messages: [{ value: encodedValue }]
                });

                console.log(`   Resubmitted: Order ${fixedOrder.orderId} sent back to 'orders' queue.`);

            } catch (e) {
                console.error("Critical error in reprocessor:", e);
            }
        },
    });
};

run().catch(console.error);
