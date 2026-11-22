import { Module, OnModuleInit } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { InjectRedis, RedisModule } from '@nestjs-modules/ioredis';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { v4 as uuidv4 } from 'uuid';

import { env } from 'src/config';
import { HealthCheckModule } from './health-check/health-check.module';
import Redis from 'ioredis';

const kafkaModule = env.flag.isRunningKafka
  ? [
      ClientsModule.register([
        {
          name: 'KAFKA_SERVICE',
          transport: Transport.KAFKA,
          options: {
            client: {
              brokers: [env.kafka.broker],
            },
            consumer: {
              groupId: `my-consumer-group-${uuidv4()}`,
            },
          },
        },
      ]),
    ]
  : [];

@Module({
  imports: [
    ScheduleModule.forRoot(),
    ConfigModule.forRoot({ envFilePath: '.env', isGlobal: true }),
    RedisModule.forRoot({
      type: 'single',
      url: env.redis.url,
      options: {},
    }),
    ...kafkaModule,
    HealthCheckModule,
  ],
})
export class AppModule implements OnModuleInit {
  constructor(@InjectRedis() private readonly redis: Redis) {}

  async onModuleInit() {}
}
