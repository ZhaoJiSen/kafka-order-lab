import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { TOPICS, type Order, type ReplayFrame, type Snapshot } from '../shared/types';

const base = process.env.LAB_URL ?? 'http://127.0.0.1:3010';
async function snapshot(): Promise<Snapshot> { return fetch(`${base}/api/lab/snapshot`).then(r => r.json()); }
async function waitForOrder(id: string, status: string) {
  const deadline = Date.now() + 35000;
  while (Date.now() < deadline) {
    const state = await snapshot(); const order = state.orders.find(o => o.orderId === id);
    if (order?.status === status && order.inbound && state.topics[1]?.partitions.find(p => p.id === order.inbound!.partition)?.committed !== null) {
      const frames: ReplayFrame[] = await fetch(`${base}/api/lab/history/${id}`).then(r => r.json());
      if (frames.some(f => f.step.index === 6)) return { state, order, frames };
    }
    await new Promise(resolve => setTimeout(resolve, 300));
  }
  throw Error(`订单 ${id} 未在时限内完成 ${status}`);
}
function verifyRecords(state: Snapshot, order: Order) {
  for (const [index, delivery] of [order.outbound, order.inbound].entries()) {
    assert.ok(delivery); assert.notEqual(delivery.offset, null);
    const partition = state.topics[index].partitions.find(p => p.id === delivery.partition)!;
    const record = partition.records.find(r => r.offset === String(delivery.offset));
    assert.ok(record, '实际 Kafka 消息必须存在');
    assert.equal(record.key, order.orderId); assert.equal(record.event?.eventId, delivery.event.eventId);
    assert.ok(BigInt(partition.committed!) > BigInt(delivery.offset!));
  }
}
let checks = 0;
for (let round = 1; round <= 3; round++) {
  for (const rejected of [false, true]) {
    const key = randomUUID();
    const body = { product: `${rejected ? '验收显示器' : '验收键盘'} · ${round}`, quantity: 1, unitPriceCents: rejected ? 120000 : 19900 };
    const send = () => fetch(`${base}/api/lab/orders`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body) });
    const response = await send(); assert.equal(response.status, 202);
    const result = await response.json();
    const reused = await (await send()).json(); assert.equal(reused.orderId, result.orderId); assert.equal(reused.reused, true);
    const { state, order, frames } = await waitForOrder(result.orderId, rejected ? 'REJECTED' : 'CONFIRMED');
    verifyRecords(state, order); assert.equal(frames[0].order.inbound, null); assert.equal(frames[0].order.status, 'PENDING_REVIEW');
    assert.equal(frames.at(-1)?.order.status, order.status);
    const before = state.topics.map(t => t.partitions.map(p => p.committed));
    await fetch(`${base}/api/lab/history/${order.orderId}`); await fetch(`${base}/api/lab/history/${order.orderId}`);
    const after = await snapshot();
    assert.deepEqual(after.topics.map(t => t.partitions.map(p => p.committed)), before, '读取回放不得改变业务组进度');
    checks++;
    console.log(`第 ${round} 轮 ${rejected ? '拒绝' : '通过'}：${order.orderId}，消息位置与提交进度一致，回放 ${frames.length} 步`);
  }
}
const final = await snapshot();
assert.ok(final.topics.every(t => t.members === 1), '观察程序不得进入业务消费组');
const kafka = new Kafka({ clientId: 'lab-verification', brokers: (process.env.KAFKA_BROKERS ?? '127.0.0.1:19092').split(','), logLevel: logLevel.NOTHING });
const admin = kafka.admin(); await admin.connect();
try {
  const offsets = await admin.fetchOffsets({ groupId: final.observerGroup, topics: [...TOPICS] });
  assert.ok(offsets.every(t => t.partitions.every(p => p.offset === '-1')), '观察消费者不能提交 Offset');
} finally { await admin.disconnect(); }
console.log(`通过 ${checks} 笔真实订单（连续 3 轮）；幂等、双向消息、业务组隔离、只读回放均通过。`);
