import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { EachMessagePayload } from 'kafkajs';
import { OrderCreatedEvent } from './events/order-created.event';
import { KafkaService } from '../services/kafka/kafka.service';

@Injectable()
export class OrdersConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OrdersConsumer.name);

  // Demo idempotence store (memory). In real life: DB unique constraint / Redis / outbox inbox table.
  private readonly processedOrderIds = new Map<string, number>();
  private cleanupTimer: NodeJS.Timeout | undefined;
  private readonly idempotenceRetentionMs = Number(
    process.env.IDEMPOTENCE_RETENTION_MS ?? 60 * 60 * 1000,
  );

  constructor(private readonly kafka: KafkaService) {}

  async onModuleInit() {
    this.cleanupTimer = setInterval(
      () => this.cleanupIdempotenceStore(),
      Math.min(this.idempotenceRetentionMs, 60_000),
    );

    await this.kafka.consumer.subscribe({
      topic: process.env.ORDERS_CREATED_TOPIC ?? 'orders.created',
      fromBeginning: false,
    });

    await this.kafka.consumer.run({
      eachMessage: async ({ message }: EachMessagePayload) => {
        if (!message.value) return;

        let parsed: unknown;
        try {
          parsed = JSON.parse(message.value.toString());
        } catch {
          this.logger.warn(
            `Invalid JSON on orders.created (offset=${message.offset})`,
          );
          return;
        }

        if (!this.isOrderCreatedEvent(parsed)) {
          this.logger.warn(
            `Invalid payload shape on orders.created (offset=${message.offset})`,
          );
          return;
        }

        await this.handleOrderCreated(parsed);
      },
    });
  }

  onModuleDestroy() {
    if (this.cleanupTimer) clearInterval(this.cleanupTimer);
  }

  private async handleOrderCreated(event: OrderCreatedEvent) {
    // ⚠️ IDEMPOTENCE: do not mark as processed BEFORE successful side-effects.
    if (this.alreadyProcessed(event.orderId)) {
      this.logger.log(`Order ${event.orderId} already processed; skipping.`);
      return;
    }

    this.logger.log(
      `Processing order ${event.orderId} for customer ${event.customerId} (amount=${event.amount})`,
    );

    try {
      // Exemple (stubs):
      // - facturation
      // - notification
      await this.simulateBilling(event);
      await this.simulateNotification(event);

      this.markProcessed(event.orderId);
      this.logger.log(`Order ${event.orderId} processed.`);
    } catch (error) {
      // Throwing keeps at-least-once semantics (message will be retried)
      this.logger.error(
        `Failed processing order ${event.orderId}`,
        error instanceof Error ? error.stack : String(error),
      );
      throw error;
    }
  }

  private simulateBilling(event: OrderCreatedEvent) {
    this.logger.debug(`Billing customer ${event.customerId}...`);
    return Promise.resolve();
  }

  private simulateNotification(event: OrderCreatedEvent) {
    this.logger.debug(`Notifying customer ${event.customerId}...`);
    return Promise.resolve();
  }

  private alreadyProcessed(orderId: string) {
    const processedAt = this.processedOrderIds.get(orderId);
    if (!processedAt) return false;

    if (Date.now() - processedAt > this.idempotenceRetentionMs) {
      this.processedOrderIds.delete(orderId);
      return false;
    }
    return true;
  }

  private markProcessed(orderId: string) {
    this.processedOrderIds.set(orderId, Date.now());
  }

  private cleanupIdempotenceStore() {
    const now = Date.now();
    for (const [orderId, processedAt] of this.processedOrderIds) {
      if (now - processedAt > this.idempotenceRetentionMs) {
        this.processedOrderIds.delete(orderId);
      }
    }
  }

  private isOrderCreatedEvent(value: unknown): value is OrderCreatedEvent {
    if (!value || typeof value !== 'object') return false;
    const v = value as Record<string, unknown>;
    return (
      typeof v.eventId === 'string' &&
      typeof v.orderId === 'string' &&
      typeof v.customerId === 'string' &&
      typeof v.amount === 'number' &&
      typeof v.createdAt === 'string'
    );
  }
}
