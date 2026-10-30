export const TOPICS = ['demo.orders.created', 'demo.orders.reviewed'] as const;
export const GROUPS = ['demo-java-order-reviewers', 'demo-php-order-results'] as const;
export type EventPayload = {
  eventId: string; eventType: string; occurredAt: string; correlationId: string;
  data: { orderId: string; status?: string; reason?: string; inputRecord?: { topic: string; partition: number; offset: number; key: string }; [key: string]: unknown };
  [key: string]: unknown;
};
export type Delivery = { topic: string; key: string; partition: number | null; offset: number | string | null; event: EventPayload; sentAt?: string | null; handledAt?: string; attempts?: number; lastError?: string | null };
export type Order = { orderId: string; product: string; quantity: number; totalCents: number; status: string; reason: string | null; createdAt: string; outbound: Delivery | null; inbound: Delivery | null; review?: { status: string; reason: string } };
export type RecordEntry = { topic: string; partition: number; offset: string; key: string; timestamp: string; event: EventPayload | null; raw: string };
export type Partition = { id: number; low: string; end: string; committed: string | null; lag: string | null; records: RecordEntry[] };
export type Topic = { name: string; groupId: string; members: number; groupState: string; partitions: Partition[] };
export type Health = { ok: boolean; checkedAt: string | null; error?: string; endpoint: string };
export type Snapshot = { capturedAt: string; health: { php: Health; java: Health; kafka: Health }; orders: Order[]; topics: Topic[]; mode: string; observerGroup: string; recordLimit: number };
export type LogEntry = { id: string; source: 'PHP' | 'Java' | 'Kafka'; level: string; at: string; text: string };
export type Step = { id: string; index: number; title: string; detail: string; at: string; source: string; partition?: number; offset?: string };
export type ReplayFrame = { id: string; observedAt: string; step: Step; order: Order; topics: Topic[] };
