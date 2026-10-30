import Fastify from 'fastify';
import { config } from './config';
import { KafkaObserver } from './kafka';
import { Logs } from './logs';
import { History } from './history';
import type { Health, Order, Snapshot } from '../shared/types';

const api = Fastify({ logger: false, bodyLimit: 32768 });
const kafka = new KafkaObserver(); const logs = new Logs(); const history = new History();
const health = (endpoint: string): Health => ({ ok: false, checkedAt: null, endpoint, error: '等待首次连接' });
let snapshot: Snapshot = { capturedAt: new Date().toISOString(), health: { php: health(config.php), java: health(config.java), kafka: health(config.brokers.join(', ')) }, orders: [], topics: [], mode: config.mode, observerGroup: kafka.groupId, recordLimit: 100 };
let polling = false;
async function json<T>(url: string): Promise<T> {
  const response = await fetch(url, { signal: AbortSignal.timeout(1800), headers: { Accept: 'application/json' } });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json() as Promise<T>;
}
async function poll() {
  if (polling) return;
  polling = true;
  const now = new Date().toISOString();
  const next = structuredClone(snapshot);
  try {
    await Promise.allSettled([
      (async () => {
        try { const result = await json<{ orders: Order[] }>(`${config.php}/api/orders`); next.orders = result.orders; next.health.php = { ...next.health.php, ok: true, checkedAt: now, error: undefined }; }
        catch (error) { next.health.php.ok = false; next.health.php.error = String(error); }
      })(),
      (async () => {
        try { await json(`${config.java}/actuator/health`); next.health.java = { ...next.health.java, ok: true, checkedAt: now, error: undefined }; }
        catch (error) { next.health.java.ok = false; next.health.java.error = String(error); }
      })(),
      (async () => {
        try { next.topics = await kafka.snapshot(); next.health.kafka = { ...next.health.kafka, ok: true, checkedAt: now, error: undefined }; }
        catch (error) { next.health.kafka.ok = false; next.health.kafka.error = String(error); }
      })(),
    ]);
    if (next.health.java.ok) {
      await Promise.allSettled(next.orders.map(async order => {
        try { const result = await json<{ review: { status: string; reason: string } }>(`${config.java}/api/reviews/${encodeURIComponent(order.orderId)}`); order.review = result.review; }
        catch { /* 404 代表尚无审核，不能展示假结果。 */ }
      }));
    }
    next.capturedAt = now; snapshot = next; history.capture(snapshot); await history.save();
  } finally { polling = false; }
}

api.get('/api/lab/snapshot', async () => snapshot);
api.get<{ Params: { id: string } }>('/api/lab/history/:id', async request => history.forOrder(request.params.id));
api.get('/api/lab/logs', async () => ({ logs: logs.entries, limit: 800 }));
api.post('/api/lab/orders', async (request, reply) => {
  const key = request.headers['idempotency-key'];
  if (typeof key !== 'string' || key.length > 100) return reply.code(422).send({ message: '请提供有效的 Idempotency-Key。' });
  try {
    const response = await fetch(`${config.php}/api/orders`, { method: 'POST', signal: AbortSignal.timeout(6000), headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(request.body) });
    const body = await response.json();
    void poll(); return reply.code(response.status).send(body);
  } catch { return reply.code(502).send({ message: '暂时无法连接 PHP。输入和本次请求编号已保留，可以重试。' }); }
});

await history.load(); logs.start();
await api.listen({ port: config.port, host: '127.0.0.1' });
console.log(`观察服务：http://127.0.0.1:${config.port} · PHP ${config.php} · Kafka ${config.brokers.join(', ')}`);
void poll();
const timer = setInterval(() => void poll(), 2000);
let stopping = false;
for (const signal of ['SIGINT', 'SIGTERM'] as const) process.on(signal, () => {
  if (stopping) return; stopping = true; clearInterval(timer); logs.stop();
  void Promise.allSettled([history.save(), kafka.stop(), api.close()]).finally(() => process.exit(0));
});
