export interface OrderCreatedEvent {
  eventId: string;
  orderId: string;
  customerId: string;
  amount: number;
  createdAt: string; // ISO date string
}
