import { ValidationPipe, Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { NestExpressApplication } from '@nestjs/platform-express';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import compression from 'compression';
import cookieParser from 'cookie-parser';
import helmet from 'helmet';
import morgan from 'morgan';
import { v4 as uuidv4 } from 'uuid';
import { env } from './config';
import { AppModule } from './modules/app.module';
import { CustomKafkaSerializer } from './modules/kafka/serializer';

const setMiddleware = (app: NestExpressApplication) => {
  app.use(helmet());

  app.enableCors({
    credentials: true,
    origin: (_, callback) => callback(null, true),
    allowedHeaders: [
      '*',
      'Authorization',
      'Content-Type',
      'X-Requested-With',
      'Wallet-Address',
      'wallet-address',
    ],
  });

  app.use(morgan('combined'));

  app.use(compression());

  app.use(cookieParser());

  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
    }),
  );
};

async function bootstrap() {
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    logger: new Logger('[]'),
  });
  app.useLogger(new Logger('APP'));
  const logger = new Logger('APP');

  app.setGlobalPrefix('api');
  setMiddleware(app);

  if (process.env.NODE_ENV !== 'production') {
    const swaggerConfig = new DocumentBuilder()
      .setTitle('MM Layer')
      .setVersion('0.0.1')
      .build();

    const swaggerDocument = SwaggerModule.createDocument(app, swaggerConfig);
    SwaggerModule.setup('swagger', app, swaggerDocument, {
      jsonDocumentUrl: 'swagger/json',
    });
  }

  if (env.flag.isRunningKafka) {
    app.connectMicroservice<MicroserviceOptions>({
      transport: Transport.KAFKA,
      options: {
        client: {
          brokers: [env.kafka.broker],
          clientId: 'app-ingestion-collector',
        },
        consumer: {
          groupId: `my-consumer-group-${uuidv4()}`,
        },
        serializer: new CustomKafkaSerializer(),
      },
    });
  }

  await app.startAllMicroservices();

  await app.listen(env.port, () =>
    logger.warn(`> Listening App on port ${env.port}`),
  );
}

bootstrap();
