import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { OrdersModule } from './orders/orders.module';
import { KafkaModule } from './services/kafka/kafka.module';

@Module({
  imports: [OrdersModule, KafkaModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
