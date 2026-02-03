import { Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { KafkaService } from '../services/kafka/kafka.service';
import { CreateOrderDto } from './dto/create-order.dto';
import { OrderCreatedEvent } from './events/order-created.event';

@Injectable()
export class OrdersService {
  constructor(private kafka: KafkaService) {}

  // Demo-only "DB". Replace with a real repository (Postgres, Mongo, etc.).
  private readonly orders = new Map<
    string,
    { orderId: string; customerId: string; amount: number; createdAt: string }
  >();

  async createOrder(dto: CreateOrderDto) {
    const orderId = randomUUID();
    const createdAt = new Date().toISOString();

    const order = {
      orderId,
      customerId: dto.customerId,
      amount: dto.amount,
      createdAt,
    };

    // 1) Save in DB (here: in-memory)
    this.orders.set(orderId, order);

    // 2) Publish Kafka event
    const event: OrderCreatedEvent = {
      eventId: randomUUID(),
      orderId,
      customerId: dto.customerId,
      amount: dto.amount,
      createdAt,
    };

    await this.kafka.producer.send({
      topic: process.env.ORDERS_CREATED_TOPIC ?? 'orders.created',
      messages: [
        {
          key: orderId,
          value: JSON.stringify(event),
          headers: {
            eventId: event.eventId,
          },
        },
      ],
    });

    return order;
  }
}
