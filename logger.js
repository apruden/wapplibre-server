import pino from 'pino';

const level = process.env.LOG_LEVEL || 'info';
const transport = process.env.NODE_ENV === 'development'
  ? {
      target: 'pino-pretty',
      options: {
        colorize: true,
        translateTime: 'SYS:standard',
        singleLine: true,
      },
    }
  : undefined;

const logger = pino({ level, transport });

export default logger;
