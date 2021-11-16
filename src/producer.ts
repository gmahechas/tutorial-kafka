import { Kafka, CompressionTypes, CompressionCodecs } from 'kafkajs';
const snappy = require('kafkajs-snappy');
CompressionCodecs[CompressionTypes.Snappy] = snappy;

const run = async () => {
	try {
		const kafka = new Kafka({
			clientId: 'producer',
			brokers: ['10.1.0.229:9090'/* , '10.1.229.9091', '10.1.229.9092' */]
		});
		const producer = kafka.producer({ idempotent: true });
		producer.on('producer.connect', () => console.log('producer.connect'));
		
		await producer.connect();

		const message = await producer.send({
			topic: 'topic1',
			acks: -1,
			compression: 2,
			messages: [{ value: '1' }]
		});
		console.log(message);

		await producer.disconnect();
	} catch (error) {
		console.error(error);
	}
}

run();