import { Module } from '@nestjs/common';
import { OrdersController } from './orders.controller';
import { OrdersConsumer } from './orders.consumer';
import { OrdersService } from './orders.service';
import { KafkaModule } from '../services/kafka/kafka.module';

@Module({
  imports: [KafkaModule],
  controllers: [OrdersController],
  providers: [OrdersService, OrdersConsumer],
})
export class OrdersModule {}
