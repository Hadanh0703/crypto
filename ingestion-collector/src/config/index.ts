import * as dotenv from 'dotenv';
import * as Joi from 'joi';
dotenv.config();

export const isLocal = process.env.NODE_ENV === 'local';

const envVarsSchema = Joi.object()
  .keys({
    NODE_ENV: Joi.string()
      .valid('production', 'development', 'test', 'local', 'staging')
      .required(),
    PORT: Joi.number().default(3000),
    WORKER_PORT: Joi.number().default(3001),

    REDIS_URL: Joi.string().required(),

    KAFKA_BROKER: Joi.string().default('localhost:39092'),
    KAFKA_TOPIC_PREFIX: Joi.string().default('local'),
    KAFKA_RUNNING_FLAG: Joi.boolean().default(false),

    COINDESK_API_KEYS: Joi.string().default(''),
  })
  .unknown();

const { value: envVars, error } = envVarsSchema
  .prefs({ errors: { label: 'key' } })
  .validate(process.env);

if (error != null) {
  throw new Error(`Config validation error: ${error.message}`);
}

export const env = {
  env: envVars.NODE_ENV,
  port: envVars.PORT,
  workerPort: envVars.WORKER_PORT,
  redis: {
    url: envVars.REDIS_URL,
  },
  kafka: {
    broker: envVars.KAFKA_BROKER,
    topicPrefix: envVars.KAFKA_TOPIC_PREFIX,
  },
  flag: {
    isRunningKafka:
      envVars.KAFKA_RUNNING_FLAG === true ||
      envVars.KAFKA_RUNNING_FLAG === 'true',
  },
  coindesk: {
    apiKeys: (envVars.COINDESK_API_KEYS as string)
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean),
  },
};
