import { Kafka, CompressionTypes, CompressionCodecs } from 'kafkajs';
const snappy = require('kafkajs-snappy');
CompressionCodecs[CompressionTypes.Snappy] = snappy;

const run = async () => {
	try {
		const kafka = new Kafka({
			clientId: 'producer',
			brokers: ['10.1.0.229:9090', '10.1.229.9091', '10.1.229.9092']
		});
		const consumer = kafka.consumer({ groupId: 'group1' });
		consumer.on('consumer.connect', () => console.log('consumer.connect'));
		await consumer.connect();

		await consumer.subscribe({ topic: 'first-topic' });
		await consumer.run({
			eachMessage: async ({ topic, partition, message }) => {
				console.log('topic:::', topic);
				console.log('partition:::', partition);
				console.log('message:::', message.value?.toString());
				console.log('key:::', message.key?.toString());
			},
		});
	} catch (error) {
		console.error(error);
	}
}

run();