import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { LogEntry, ReplayFrame, Snapshot } from '../shared/types';

type Form = { product: string; quantity: string; price: string };
type LabState = {
  snapshot: Snapshot | null; logs: LogEntry[]; selected: string; frames: ReplayFrame[];
  replayFrames: ReplayFrame[]; mode: 'live' | 'replay'; cursor: number; playing: boolean; speed: number;
  form: Form; request: { fingerprint: string; key: string } | null; error: string; feedback: string; feedbackKind: 'info' | 'success' | 'error'; busy: boolean;
  historyLoading: boolean; historyError: string; loadHistory: (id: string) => Promise<void>;
  setForm: (patch: Partial<Form>) => void; select: (id: string) => void; refresh: () => Promise<void>;
  submit: () => Promise<void>; replay: () => void; live: () => void; move: (index: number) => void;
  setPlaying: (playing: boolean) => void; setSpeed: (speed: number) => void;
};
let refreshing = false;
let historyRequest = 0;
async function get<T>(url: string): Promise<T> {
  const r = await fetch(url, { signal: AbortSignal.timeout(5000) });
  if (!r.ok) throw Error('观察服务暂时无法连接，正在自动重试。');
  return r.json();
}
export const useLab = create<LabState>()(persist((set, state) => ({
  snapshot: null, logs: [], selected: '', frames: [], replayFrames: [], mode: 'live', cursor: 0, playing: false, speed: 1,
  form: { product: '机械键盘', quantity: '2', price: '199.00' }, request: null, error: '', feedback: '', feedbackKind: 'info', busy: false,
  historyLoading: false, historyError: '',
  setForm: patch => set(s => ({ form: { ...s.form, ...patch }, feedback: '' })),
  select: id => {
    if (id === state().selected) return;
    set({ selected: id, mode: 'live', playing: false, cursor: 0, frames: [], replayFrames: [], feedback: '' });
    void state().loadHistory(id);
  },
  loadHistory: async id => {
    const request = ++historyRequest;
    set({ historyLoading: state().frames.length === 0, historyError: '' });
    try {
      const frames = await get<ReplayFrame[]>(`/api/lab/history/${encodeURIComponent(id)}`);
      if (state().selected === id && request === historyRequest) set({ frames: frames.filter(f => f.order.orderId === id), historyLoading: false });
    } catch {
      if (state().selected === id && request === historyRequest) set({ historyLoading: false, historyError: '回放记录暂未读取到，正在重试。' });
    }
  },
  refresh: async () => {
    if (refreshing) return; refreshing = true;
    try {
      const snapshot = await get<Snapshot>('/api/lab/snapshot');
      const currentSelection = state().selected;
      const awaitingNewOrder = state().busy || state().feedback.startsWith('订单已保存');
      const selected = currentSelection && (snapshot.orders.some(o => o.orderId === currentSelection) || awaitingNewOrder || !snapshot.health.php.ok)
        ? currentSelection : snapshot.orders[0]?.orderId || '';
      if (selected !== currentSelection) set({ frames: [], replayFrames: [], cursor: 0, mode: 'live', playing: false });
      set({ snapshot, selected, error: '' });
      const current = snapshot.orders.find(o => o.orderId === selected);
      if (current && current.status !== 'PENDING_REVIEW' && state().feedback.startsWith('订单已保存')) {
        set({ feedback: current.status === 'CONFIRMED' ? '审核通过，PHP 已保存结果。' : '审核拒绝，PHP 已保存原因。', feedbackKind: current.status === 'CONFIRMED' ? 'success' : 'info' });
      }
      const results = await Promise.allSettled([
        get<{ logs: LogEntry[] }>('/api/lab/logs'),
        selected ? state().loadHistory(selected) : Promise.resolve(),
      ]);
      if (results[0].status === 'fulfilled') set({ logs: results[0].value.logs });
    } catch (error) { set({ error: (error as Error).message }); }
    finally { refreshing = false; }
  },
  submit: async () => {
    if (state().busy) return;
    const form = state().form;
    if (!form.product.trim() || !/^\d+$/.test(form.quantity) || Number(form.quantity) < 1 || Number(form.quantity) > 1000 || !/^\d+(\.\d{1,2})?$/.test(form.price) || Number(form.price) < 0.01 || Number(form.price) > 100000) {
      set({ feedback: '请填写商品名称、1～1000 的整数数量，以及最多两位小数的有效单价。', feedbackKind: 'error' });
      return;
    }
    const [whole, fraction = ''] = form.price.split('.');
    const body = { product: form.product.trim(), quantity: Number(form.quantity), unitPriceCents: Number(whole) * 100 + Number(fraction.padEnd(2, '0')) };
    const fingerprint = JSON.stringify(body);
    const request = state().request?.fingerprint === fingerprint ? state().request! : { fingerprint, key: crypto.randomUUID() };
    set({ busy: true, request, feedback: '', feedbackKind: 'info' });
    try {
      const response = await fetch('/api/lab/orders', { method: 'POST', signal: AbortSignal.timeout(8000), headers: { 'Content-Type': 'application/json', 'Idempotency-Key': request.key }, body: JSON.stringify(body) });
      const result = await response.json();
      if (!response.ok) throw new Error(Object.values(result.errors ?? {}).flat().join('；') || result.message || '创建失败，请重试。');
      set({ selected: result.orderId, request: null, mode: 'live', playing: false, cursor: 0, frames: [], replayFrames: [], historyError: '', feedback: '订单已保存，正在观察后续消息。' });
      void state().loadHistory(result.orderId);
      await state().refresh();
    } catch (error) { set({ feedback: error instanceof TypeError || error instanceof DOMException ? '暂时无法连接观察服务。输入和本次请求编号已保留，请重试。' : (error as Error).message, feedbackKind: 'error' }); }
    finally { set({ busy: false }); }
  },
  replay: () => {
    const frames = state().frames.filter(f => f.order.orderId === state().selected);
    if (frames.length) set({ mode: 'replay', replayFrames: structuredClone(frames), cursor: 0, playing: false });
  },
  live: () => set({ mode: 'live', playing: false }),
  move: cursor => set(s => ({ cursor: Math.max(0, Math.min(cursor, Math.max(0, s.replayFrames.length - 1))) })),
  setPlaying: playing => set({ playing }), setSpeed: speed => set({ speed }),
}), { name: 'order-message-lab-v1', partialize: s => ({ selected: s.selected, form: s.form, request: s.request }) }));
