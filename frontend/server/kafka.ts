import { Kafka, logLevel, type Consumer } from 'kafkajs';
import { GROUPS, TOPICS, type RecordEntry, type Topic } from '../shared/types';
import { lagFor } from '../shared/model';
import { config } from './config';

export class KafkaObserver {
  groupId = `order-lab-observer-${process.pid}`;
  private kafka = new Kafka({ clientId: 'order-lab-observer', brokers: config.brokers, logLevel: logLevel.NOTHING, connectionTimeout: 1500, requestTimeout: 3500, retry: { retries: 1 } });
  private admin = this.kafka.admin();
  private consumer?: Consumer;
  private starting = false;
  private closed = false;
  private records = new Map<string, RecordEntry[]>();
  private bounds = new Map<string, { low: string; high: string }>();
  async snapshot(): Promise<Topic[]> {
    await this.admin.connect();
    const result: Topic[] = [];
    for (const [index, topic] of TOPICS.entries()) {
      const offsets = (await this.admin.fetchTopicOffsets(topic)).sort((a, b) => a.partition - b.partition);
      for (const p of offsets) this.bounds.set(`${topic}:${p.partition}`, { low: p.low, high: p.high });
      let committed = new Map<number, string>();
      let members = 0; let groupState = '尚未运行';
      // 消费组查询失败必须标记未知，不能当作已追平或已提交 0。
      try {
        const response = await this.admin.fetchOffsets({ groupId: GROUPS[index], topics: [topic] });
        committed = new Map(response.flatMap(t => t.partitions.map(p => [p.partition, p.offset] as const)));
        const description = await this.admin.describeGroups([GROUPS[index]]);
        const group = description.groups[0];
        members = group?.members.length ?? 0; groupState = group?.state ?? '未知';
      } catch { groupState = '消费进度暂不可用'; }
      result.push({ name: topic, groupId: GROUPS[index], members, groupState, partitions: offsets.map(p => {
        const offset = committed.get(p.partition);
        const next = offset === undefined || offset === '-1' ? null : offset;
        return { id: p.partition, low: p.low, end: p.high, committed: next, lag: lagFor(p.high, next), records: [...(this.records.get(`${topic}:${p.partition}`) ?? [])] };
      }) });
    }
    if (!this.consumer && !this.starting) void this.startConsumer();
    return result;
  }
  private async startConsumer() {
    this.starting = true;
    const consumer = this.kafka.consumer({ groupId: this.groupId, allowAutoTopicCreation: false, sessionTimeout: 15000, retry: { retries: 2 } });
    let ready = false;
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
      for (const [topic, partitions] of Object.entries(payload.memberAssignment)) {
        for (const partition of partitions) {
          const b = this.bounds.get(`${topic}:${partition}`);
          if (!b) continue;
          const start = BigInt(b.high) - 100n;
          consumer.seek({ topic, partition, offset: (start > BigInt(b.low) ? start : BigInt(b.low)).toString() });
        }
      }
      ready = true;
    });
    consumer.on(consumer.events.CRASH, () => { if (this.consumer === consumer) { this.consumer = undefined; void consumer.disconnect().catch(() => undefined); } });
    try {
      await consumer.connect();
      await consumer.subscribe({ topics: [...TOPICS], fromBeginning: true });
      await consumer.run({ autoCommit: false, eachMessage: async ({ topic, partition, message }) => {
        if (!ready) return;
        const raw = message.value?.toString() ?? '';
        let event = null;
        try { event = JSON.parse(raw); } catch { /* 非 JSON 原文仍然展示。 */ }
        const record: RecordEntry = { topic, partition, offset: message.offset, key: message.key?.toString() ?? '', timestamp: new Date(Number(message.timestamp)).toISOString(), event, raw };
        const id = `${topic}:${partition}`;
        const records = this.records.get(id) ?? [];
        if (!records.some(r => r.offset === record.offset)) records.push(record);
        records.sort((a, b) => BigInt(a.offset) < BigInt(b.offset) ? -1 : 1);
        this.records.set(id, records.slice(-100));
      } });
      if (this.closed) await consumer.disconnect(); else this.consumer = consumer;
    } catch { await consumer.disconnect().catch(() => undefined); }
    finally { this.starting = false; }
  }
  async stop() { this.closed = true; await this.consumer?.disconnect(); await this.admin.disconnect(); }
}
