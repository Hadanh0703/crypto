import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { Kafka } from 'kafkajs';
import { env } from 'src/config';
import { KafkaTopics } from './schema';

@Injectable()
export class WorkerKafkaService implements OnModuleInit {
  private logger = new Logger(WorkerKafkaService.name);

  constructor(
    @Inject('KAFKA_SERVICE') private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    if (!env.flag.isRunningKafka) {
      return;
    }

    const kafka = new Kafka({
      brokers: [env.kafka.broker],
    });
    const admin = kafka.admin();
    await admin.connect();

    try {
      const topics = await admin.listTopics();
      const topicList = [];

      for (const topic of Object.values(KafkaTopics)) {
        this.logger.log(topic);
        if (!topics.includes(topic)) {
          topicList.push({
            topic,
            numPartitions: 3,
            replicationFactor: 1,
          });
        }
      }

      if (topicList.length) {
        this.logger.log('Creating topics', topicList);
        await admin.createTopics({
          topics: topicList,
        });
      }
    } finally {
      await admin.disconnect();
    }

    await this.kafkaClient.connect();
  }

  emitMessage<M>(topic: string, message: M) {
    return this.kafkaClient.emit(topic, message);
  }
}
