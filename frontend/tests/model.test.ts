import { test } from 'node:test';
import assert from 'node:assert/strict';
import { lagFor, partitionForKey, revealOrder, revealTopics, stepsFor } from '../shared/model';
import { TOPICS, GROUPS, type Order, type Topic } from '../shared/types';

const order: Order = { orderId: 'ord-test', product: '键盘', quantity: 1, totalCents: 9900, status: 'CONFIRMED', reason: '通过', createdAt: '2026-10-27 05:00:00', outbound: { topic: TOPICS[0], key: 'ord-test', partition: 0, offset: 3, sentAt: '2026-10-27 05:00:01', event: { eventId: 'evt-create', eventType: 'OrderCreated', occurredAt: '2026-10-27T05:00:00Z', correlationId: 'ord-test', data: { orderId: 'ord-test' } } }, inbound: { topic: TOPICS[1], key: 'ord-test', partition: 1, offset: 9, handledAt: '2026-10-27 05:00:03', event: { eventId: 'evt-result', eventType: 'OrderReviewed', occurredAt: '2026-10-27T05:00:02Z', correlationId: 'ord-test', data: { orderId: 'ord-test', status: 'CONFIRMED', reason: '通过', inputRecord: { topic: TOPICS[0], key: 'ord-test', partition: 0, offset: 3 } } } } };
const topics: Topic[] = TOPICS.map((name, i) => ({ name, groupId: GROUPS[i], members: 1, groupState: 'Stable', partitions: [{ id: i, low: '0', end: i ? '10' : '4', committed: i ? '10' : '4', lag: '0', records: [] }] }));

test('PHP CRC32 分区结果与已确认的真实日志对应', () => {
  assert.deepEqual(partitionForKey('ord-bbac5335-4f42-4989-8bd7-8252fb60ef87', 3), { hash: 1169837243, partition: 2 });
  assert.equal(partitionForKey('ord-9a6feb50-eff4-4340-8add-cfdebaa9adbc', 3).partition, 0);
  assert.equal(partitionForKey('same-key', 0).partition, null);
});
test('64 位 Offset 位置差保持精度，未知进度不冒充 0', () => {
  assert.equal(lagFor('90071992547409930', '90071992547409927'), '3');
  assert.equal(lagFor('10', null), null);
  assert.equal(lagFor('10', '-1'), null);
  assert.equal(lagFor('3', '4'), '0');
});
test('已保存结果和真实提交位置合起来才能证明 PHP 提交步骤', () => {
  const done = stepsFor(order, topics, '2026-10-27T05:00:04Z');
  assert.deepEqual(done.map(s => s.index), [0, 1, 2, 3, 4, 5, 6]);
  const notSaved = stepsFor({ ...order, inbound: null }, topics, 'now');
  assert.equal(notSaved.some(s => s.index === 6), false);
  const notCommitted = structuredClone(topics); notCommitted[1].partitions[0].committed = '9';
  assert.equal(stepsFor(order, notCommitted, 'now').some(s => s.index === 6), false);
});
test('早期回放不会泄露后来审核结果，也不修改原始订单', () => {
  const initial = revealOrder(order, 0);
  assert.equal(initial.status, 'PENDING_REVIEW'); assert.equal(initial.inbound, null); assert.equal(initial.outbound?.offset, null);
  assert.equal(order.status, 'CONFIRMED'); assert.equal(order.inbound?.offset, 9);
  assert.equal(revealTopics(topics, order.orderId, 0)[1].partitions[0].committed, null);
  assert.equal(topics[1].partitions[0].committed, '10');
});
