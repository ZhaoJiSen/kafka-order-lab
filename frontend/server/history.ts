import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { stepsFor, revealOrder, revealTopics } from '../shared/model';
import type { ReplayFrame, Snapshot } from '../shared/types';
import { config } from './config';

export class History {
  frames: ReplayFrame[] = [];
  private dirty = false;
  async load() {
    try { this.frames = JSON.parse(await readFile(join(config.local, 'history.json'), 'utf8')); }
    catch { this.frames = []; }
  }
  capture(snapshot: Snapshot) {
    const known = new Set(this.frames.map(f => f.id));
    for (const order of snapshot.orders) {
      for (const step of stepsFor(order, snapshot.topics, snapshot.health.kafka.checkedAt ?? snapshot.capturedAt)) {
        if (known.has(step.id)) continue;
        this.frames.push({ id: step.id, observedAt: snapshot.capturedAt, step,
          order: revealOrder(order, step.index), topics: this.nearby(revealTopics(snapshot.topics, order.orderId, step.index), order.orderId) });
        known.add(step.id); this.dirty = true;
      }
    }
    if (this.frames.length > 2000) this.frames = this.frames.slice(-2000);
  }
  private nearby(topics: ReplayFrame['topics'], orderId: string) {
    return topics.map(t => ({ ...t, partitions: t.partitions.map(p => {
      const index = p.records.findIndex(r => r.key === orderId);
      return { ...p, records: index >= 0 ? p.records.slice(Math.max(0, index - 2), index + 3) : p.records.slice(-3) };
    }) }));
  }
  async save() {
    if (!this.dirty) return;
    this.dirty = false;
    try {
      await mkdir(config.local, { recursive: true });
      const target = join(config.local, 'history.json');
      await writeFile(target + '.tmp', JSON.stringify(this.frames));
      await rename(target + '.tmp', target);
    } catch { this.dirty = true; }
  }
  forOrder(id: string) { return this.frames.filter(f => f.order.orderId === id).sort((a, b) => a.step.index - b.step.index); }
}
