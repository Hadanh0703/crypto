import { Module, OnModuleInit } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { InjectRedis, RedisModule } from '@nestjs-modules/ioredis';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { env } from 'src/config';
import { HealthCheckModule } from '../health-check/health-check.module';
import Redis from 'ioredis';
import { WorkerService } from './worker.service';
import { CustomKafkaSerializer } from '../kafka/serializer';
import { WorkerKafkaService } from '../kafka/worker-kafka.service';

const kafkaModule = env.flag.isRunningKafka
  ? [
      ClientsModule.register([
        {
          name: 'KAFKA_SERVICE',
          transport: Transport.KAFKA,
          options: {
            client: {
              brokers: [env.kafka.broker],
              clientId: 'worker-ingestion-collector',
            },
            consumer: {
              groupId: undefined,
            },
            serializer: new CustomKafkaSerializer(),
          },
        },
      ]),
    ]
  : [];

const kafkaProviders = env.flag.isRunningKafka ? [WorkerKafkaService] : [];

@Module({
  imports: [
    ScheduleModule.forRoot(),
    RedisModule.forRoot({
      type: 'single',
      url: env.redis.url,
      options: {},
    }),
    ...kafkaModule,
    HealthCheckModule,
  ],
  providers: [WorkerService, ...kafkaProviders],
})
export class WorkerModule implements OnModuleInit {
  constructor(@InjectRedis() private readonly redis: Redis) {}

  async onModuleInit() {}
}
