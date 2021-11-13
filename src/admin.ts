import { Kafka, ConfigResourceTypes } from 'kafkajs';

const run = async () => {
	try {
		const kafka = new Kafka({
			clientId: 'producer',
			brokers: ['10.1.0.229:9090', '10.1.229.9091', '10.1.229.9092']
		});
		const admin = kafka.admin();
		admin.on('admin.connect', () => console.log('admin.connect'));
		await admin.connect();

		const describeCluster = await admin.describeCluster();
		const listTopics = await admin.listTopics();
		const listGroups = await admin.listGroups();
		const describeGroups = await admin.describeGroups(['group1']);

		console.log('describeCluster:::', describeCluster);
		console.log('listTopics:::', listTopics);
		console.log('listGroups:::', listGroups);
		console.log('describeGroups:::', describeGroups.groups);


		await admin.disconnect();
	} catch (error) {
		console.error(error);
	}
}

export const admin = run;