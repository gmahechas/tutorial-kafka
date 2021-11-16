import { Kafka, CompressionTypes, CompressionCodecs, MemberDescription } from 'kafkajs';
const snappy = require('kafkajs-snappy');
CompressionCodecs[CompressionTypes.Snappy] = snappy;

const run = async () => {
	try {
		const kafka = new Kafka({
			clientId: 'lags',
			brokers: ['10.1.0.229:9090'/* , '10.1.229.9091', '10.1.229.9092' */]
		});
		const admin = kafka.admin();
		admin.on('admin.connect', () => console.log('admin.connect'));
		await admin.connect();

		const groupsList = await admin.listGroups();
		const describeGroups = await admin.describeGroups(groupsList.groups.map(group => group.groupId))

		for (const { groupId, members } of describeGroups.groups) {
			handleLagsByGroup(groupId, members);
		}


		await admin.disconnect();
	} catch (error) {
		console.error(error);
		run();
	}
}

const handleLagsByGroup = async (groupId: string, members: MemberDescription[]) => {
	const kafka = new Kafka({
		clientId: 'admin',
		brokers: ['10.1.0.229:9090'/* , '10.1.229.9091', '10.1.229.9092' */]
	});
	const consumer = kafka.consumer({ groupId });
	consumer.on('consumer.connect', () => console.log(`consumer.connect to ${groupId}`));
	
	await consumer.connect();
	await consumer.subscribe({ topic: 'topic1', fromBeginning: true });

	await consumer.run({
		eachMessage: async ({ topic, partition, message }) => {
			console.log({
				offset: message.offset,
				message: message.value?.toString()
			});
		},
	});
/* 	await consumer.disconnect(); */
};

run();