import CRC32 from 'crc-32';
import { TOPICS, type Order, type RecordEntry, type Step, type Topic } from './types';

export const STATUS: Record<string, string> = { PENDING_REVIEW: '等待审核', CONFIRMED: '审核通过', REJECTED: '审核拒绝' };
export const money = (n: number) => new Intl.NumberFormat('zh-CN', { style: 'currency', currency: 'CNY' }).format(n / 100);
export function dateLabel(value: string | null | undefined) {
  if (!value) return '尚未记录';
  const normalized = value.includes('T') ? value : value.replace(' ', 'T') + 'Z';
  const d = new Date(normalized);
  return Number.isNaN(d.getTime()) ? value : d.toLocaleTimeString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false });
}
export function partitionForKey(key: string, count: number) {
  const hash = CRC32.str(key) >>> 0;
  return { hash, partition: count > 0 ? hash % count : null };
}
export function lagFor(end: string, committed: string | null) {
  if (committed === null || committed === '-1') return null;
  const difference = BigInt(end) - BigInt(committed);
  return (difference > 0n ? difference : 0n).toString();
}
export function findRecord(topics: Topic[], orderId: string, topic: string): RecordEntry | undefined {
  return topics.find(t => t.name === topic)?.partitions.flatMap(p => p.records).find(r => r.key === orderId);
}
export function stepsFor(order: Order, topics: Topic[], observedAt: string): Step[] {
  const created = findRecord(topics, order.orderId, TOPICS[0]);
  const reviewed = findRecord(topics, order.orderId, TOPICS[1]);
  const input = reviewed?.event?.data.inputRecord ?? order.inbound?.event.data.inputRecord;
  const resultAt = reviewed?.event?.occurredAt ?? order.inbound?.event.occurredAt;
  const steps: Step[] = [{ id: `${order.orderId}:0`, index: 0, title: 'PHP 保存订单', detail: '订单与待发送事件在同一事务中保存。', at: order.createdAt, source: 'PHP 数据库' }];
  const add = (index: number, title: string, detail: string, at: string, source: string, partition?: number, offset?: string) => steps.push({ id: `${order.orderId}:${index}`, index, title, detail, at, source, partition, offset });
  const first = created ?? (order.outbound?.sentAt ? order.outbound : null);
  if (first && first.partition !== null && first.offset !== null) add(1, 'OrderCreated 写入 Kafka', '分区内保存记录，Kafka 分配这条记录的 Offset。', created?.timestamp ?? order.outbound?.sentAt ?? observedAt, created ? 'Kafka 实际记录' : 'PHP 写入确认', first.partition, String(first.offset));
  if (order.review || reviewed || order.inbound) add(2, 'Java 完成订单审核', order.review?.reason ?? reviewed?.event?.data.reason ?? order.reason ?? '审核结果已保存，并准备结果事件。', resultAt ?? observedAt, resultAt ? 'OrderReviewed 事件时间' : 'Java HTTP · 采集时已完成', input?.partition, input ? String(input.offset) : undefined);
  const inputPartition = input?.partition ?? first?.partition;
  const inputOffset = input?.offset ?? first?.offset;
  const javaCommit = topics[0]?.partitions.find(p => p.id === inputPartition)?.committed;
  if (inputOffset != null && javaCommit != null && BigInt(javaCommit) > BigInt(inputOffset)) add(3, 'Java 消费组已提交进度', `采集时下一位置为 ${javaCommit}，已越过本条记录。`, observedAt, 'Kafka 消费组查询', inputPartition ?? undefined, javaCommit);
  if (reviewed) add(4, 'OrderReviewed 写入 Kafka', '结果写入另一个 Topic，分区与 Offset 独立计算。', reviewed.timestamp, 'Kafka 实际记录', reviewed.partition, reviewed.offset);
  else if (order.inbound) add(4, 'OrderReviewed 写入 Kafka', 'PHP 接收记录可证明该结果已经写入 Kafka。', resultAt ?? observedAt, 'PHP 接收记录', order.inbound.partition ?? undefined, String(order.inbound.offset));
  if (order.inbound) add(5, 'PHP 保存审核结果', `订单更新为「${STATUS[order.status] ?? order.status}」。`, order.inbound.handledAt ?? observedAt, 'PHP 数据库');
  const resultPartition = order.inbound?.partition ?? reviewed?.partition;
  const resultOffset = order.inbound?.offset ?? reviewed?.offset;
  const phpCommit = topics[1]?.partitions.find(p => p.id === resultPartition)?.committed;
  if (order.inbound && resultOffset != null && phpCommit != null && BigInt(phpCommit) > BigInt(resultOffset)) add(6, 'PHP 消费组已提交进度', `采集时下一位置为 ${phpCommit}；消息仍保留在分区中。`, observedAt, 'Kafka 消费组查询', resultPartition ?? undefined, phpCommit);
  return steps.sort((a, b) => a.index - b.index);
}

// 回放是按真实证据逐步揭示；不能把尚未揭示的结果提前带入画面。
export function revealOrder(order: Order, index: number): Order {
  return { ...order, status: index >= 5 ? order.status : 'PENDING_REVIEW', reason: index >= 5 ? order.reason : null, inbound: index >= 5 ? order.inbound : null, review: index >= 2 ? order.review : undefined, outbound: order.outbound ? { ...order.outbound, sentAt: index >= 1 ? order.outbound.sentAt : null, partition: index >= 1 ? order.outbound.partition : null, offset: index >= 1 ? order.outbound.offset : null } : null };
}
export function revealTopics(topics: Topic[], orderId: string, index: number): Topic[] {
  return topics.map((t, ti) => ({ ...t, partitions: t.partitions.map(p => ({ ...p,
    committed: index >= (ti === 0 ? 3 : 6) ? p.committed : null,
    lag: index >= (ti === 0 ? 3 : 6) ? p.lag : null,
    records: p.records.filter(r => r.key !== orderId || index >= (ti === 0 ? 1 : 4)),
  })) }));
}
