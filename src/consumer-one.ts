import { Kafka, CompressionTypes, CompressionCodecs } from 'kafkajs';
const snappy = require('kafkajs-snappy');
CompressionCodecs[CompressionTypes.Snappy] = snappy;

const run = async () => {
	try {
		const kafka = new Kafka({
			clientId: `ms-1-expressjs-1`,
			brokers: ['10.1.0.229:9090'/* , '10.1.229.9091', '10.1.229.9092' */]
		});
		const consumer = kafka.consumer({ groupId: 'group1', heartbeatInterval: 1000 });
		const showData = false;
		consumer.on('consumer.heartbeat', (data) => console.log((showData) ? data : '', 'consumer.heartbeat'));
		consumer.on('consumer.commit_offsets', (data) => console.log((showData) ? data : '', 'consumer.commit_offsets'));
		consumer.on('consumer.group_join', (data) => console.log((showData) ? data : '', 'consumer.group_join'));
		consumer.on('consumer.fetch_start', (data) => console.log((showData) ? data : '', 'consumer.fetch_start'));
		consumer.on('consumer.fetch', (data) => console.log((showData) ? data : '', 'consumer.fetch'));
		consumer.on('consumer.start_batch_process', (data) => console.log((showData) ? data : '', 'consumer.start_batch_process'));
		consumer.on('consumer.end_batch_process', (data) => console.log((showData) ? data : '', 'consumer.end_batch_process'));
		consumer.on('consumer.connect', () => console.log('consumer.connect'));
		consumer.on('consumer.disconnect', () => { console.log('consumer.disconnect'); process.exit(); });
		consumer.on('consumer.stop', () => { console.log('consumer.stop'); });
		consumer.on('consumer.crash', (data) => console.log((showData) ? data : '', 'consumer.crash'));
		consumer.on('consumer.received_unsubscribed_topics', (data) => console.log((showData) ? data : '', 'consumer.received_unsubscribed_topics'));
		consumer.on('consumer.network.request', (data) => console.log((showData) ? data : '', 'consumer.network.request'));
		consumer.on('consumer.network.request_timeout', (data) => console.log((showData) ? data : '', 'consumer.network.request_timeout'));
		consumer.on('consumer.network.request_queue_size', (data) => console.log((showData) ? data : '', 'consumer.network.request_queue_size'));
		await consumer.connect();

		await consumer.subscribe({ topic: 'topic1', fromBeginning: true });
		await consumer.run({
			/* autoCommit: false, */
			eachBatchAutoResolve: false,
			eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
				for (let message of batch.messages) {
					const value = message.value!.toString();
					console.log('processing: ', value);
					resolveOffset(message.offset);
					/* await commitOffsetsIfNecessary({
						topics: [
							{
								topic: 'topic1',
								partitions: [
									{
										partition: batch.partition,
										offset: (Number(message.offset) + 1).toString(),
									}
								]
							}
						]
					}); */
					await heartbeat();
				}
			}
		});
		/* consumer.seek({ topic: 'topic1', partition: 0, offset: '4' }); */
		process.on('SIGINT', async () => { await consumer.stop(); await consumer.disconnect(); });
		process.on('SIGTERM', async () => { await consumer.stop(); await consumer.disconnect(); });
	} catch (error) {
		console.error(error);
	}
}

run();