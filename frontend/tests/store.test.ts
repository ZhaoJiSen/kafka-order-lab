import { after, beforeEach, test } from 'node:test';
import assert from 'node:assert/strict';
import type { ReplayFrame, Order, Snapshot } from '../shared/types';

// 隔离浏览器存储与请求，不连接本机业务服务。
const savedStorage = Object.getOwnPropertyDescriptor(globalThis, 'localStorage');
const savedWindow = Object.getOwnPropertyDescriptor(globalThis, 'window');
const storage = new Map<string, string>();
Object.defineProperty(globalThis, 'localStorage', { configurable: true, value: {
  getItem: (key: string) => storage.get(key) ?? null,
  setItem: (key: string, value: string) => storage.set(key, value),
  removeItem: (key: string) => storage.delete(key),
} });
Object.defineProperty(globalThis, 'window', { configurable: true, value: { localStorage: globalThis.localStorage } });
const realFetch = globalThis.fetch;
const { useLab } = await import('../src/store');
const initial = useLab.getInitialState();
const response = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } });
const order = (id: string): Order => ({ orderId: id, product: id, quantity: 1, totalCents: 19900, status: 'CONFIRMED', reason: '通过', createdAt: '2026-10-27T10:00:00Z', outbound: null, inbound: null });
const frame = (id: string, index = 0): ReplayFrame => ({ id: `${id}:${index}`, order: order(id), topics: [], observedAt: '2026-10-27T10:00:00Z', step: { id: `${id}:${index}`, index, title: '保存订单', detail: '', at: '2026-10-27T10:00:00Z', source: '数据库' } });
const snapshot = (id: string): Snapshot => ({ capturedAt: '2026-10-27T10:00:00Z', health: { php: { ok: true, checkedAt: null, endpoint: '' }, java: { ok: true, checkedAt: null, endpoint: '' }, kafka: { ok: true, checkedAt: null, endpoint: '' } }, orders: [order(id)], topics: [], mode: 'native', observerGroup: '', recordLimit: 100 });
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}
beforeEach(() => { storage.clear(); useLab.setState(initial, true); });
after(() => {
  globalThis.fetch = realFetch;
  if (savedStorage) Object.defineProperty(globalThis, 'localStorage', savedStorage);
  else Reflect.deleteProperty(globalThis, 'localStorage');
  if (savedWindow) Object.defineProperty(globalThis, 'window', savedWindow);
  else Reflect.deleteProperty(globalThis, 'window');
});

test('切换订单后，较晚返回的旧回放不能覆盖当前订单', async () => {
  const delayed = deferred<Response>();
  globalThis.fetch = async input => String(input).endsWith('/A') ? delayed.promise : response([frame('B')]);
  useLab.setState({ selected: 'A', frames: [frame('A')] });
  const oldRequest = useLab.getState().loadHistory('A');
  useLab.getState().select('B');
  await useLab.getState().loadHistory('B');
  delayed.resolve(response([frame('A')]));
  await oldRequest;
  assert.equal(useLab.getState().selected, 'B');
  assert.deepEqual(useLab.getState().frames.map(f => f.order.orderId), ['B']);
  useLab.getState().replay();
  assert.equal(useLab.getState().replayFrames[0].order.orderId, 'B');
});

test('创建成功时立即清除上一订单回放，快照未返回也不能重播旧订单', async () => {
  const pendingSnapshot = deferred<Response>();
  globalThis.fetch = async (input, options) => {
    if (options?.method === 'POST') return response({ orderId: 'B' }, 202);
    if (String(input).endsWith('/snapshot')) return pendingSnapshot.promise;
    return response(String(input).endsWith('/logs') ? { logs: [] } : []);
  };
  useLab.setState({ selected: 'A', frames: [frame('A')], replayFrames: [frame('A')], mode: 'replay' });
  const submit = useLab.getState().submit();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(useLab.getState().selected, 'B');
  assert.deepEqual(useLab.getState().frames, []);
  assert.deepEqual(useLab.getState().replayFrames, []);
  useLab.getState().replay();
  assert.equal(useLab.getState().mode, 'live');
  pendingSnapshot.resolve(response(snapshot('B')));
  await submit;
});

test('失败后保留输入与请求编号，同样内容重试复用幂等键', async () => {
  const keys: string[] = [];
  globalThis.fetch = async (_input, options) => {
    keys.push((options?.headers as Record<string, string>)['Idempotency-Key']);
    return response({ message: '连接失败，请重试。' }, 503);
  };
  useLab.getState().setForm({ product: '显示器', quantity: '1', price: '1200.00' });
  await useLab.getState().submit();
  const failed = useLab.getState();
  assert.equal(failed.busy, false);
  assert.equal(failed.feedbackKind, 'error');
  assert.equal(failed.form.price, '1200.00');
  assert.ok(failed.request?.key);
  const persisted = JSON.parse(storage.get('order-message-lab-v1')!);
  assert.equal(persisted.state.request.key, failed.request.key);
  await failed.submit();
  assert.equal(keys.length, 2);
  assert.equal(keys[0], keys[1]);
});

test('回放固定为进入时的证据，后续采集不会悄悄推进', () => {
  useLab.setState({ selected: 'A', frames: [frame('A'), frame('A', 1)] });
  useLab.getState().replay();
  useLab.setState({ frames: [frame('A'), frame('A', 1), frame('A', 2)] });
  assert.equal(useLab.getState().replayFrames.length, 2);
  useLab.getState().move(100);
  assert.equal(useLab.getState().cursor, 1);
  useLab.getState().replay();
  assert.equal(useLab.getState().replayFrames.length, 3);
  assert.equal(useLab.getState().cursor, 0);
});

test('金额按分转换，非法小数不截断后提交', async () => {
  let calls = 0;
  globalThis.fetch = async () => { calls++; return response({}, 500); };
  useLab.getState().setForm({ price: '1.999' });
  await useLab.getState().submit();
  assert.equal(calls, 0);
  assert.equal(useLab.getState().feedbackKind, 'error');
});
