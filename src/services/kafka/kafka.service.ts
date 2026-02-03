import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Kafka, logLevel } from 'kafkajs';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaService.name);

  private readonly kafka = new Kafka({
    brokers: (process.env.KAFKA_BROKERS ?? 'kafka:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter(Boolean),
    logLevel: logLevel.NOTHING,
  });

  readonly producer = this.kafka.producer();
  readonly consumer = this.kafka.consumer({
    groupId: process.env.KAFKA_CONSUMER_GROUP_ID ?? 'orders-group',
  });

  async onModuleInit() {
    await this.producer.connect();
    await this.consumer.connect();
    this.logger.log('Kafka producer/consumer connected');
  }

  async onModuleDestroy() {
    await Promise.allSettled([
      this.consumer.disconnect(),
      this.producer.disconnect(),
    ]);
    this.logger.log('Kafka producer/consumer disconnected');
  }
}
