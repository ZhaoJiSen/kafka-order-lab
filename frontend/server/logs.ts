import { spawn, type ChildProcess } from 'node:child_process';
import { open, stat } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { config } from './config';
import type { LogEntry } from '../shared/types';

export class Logs {
  entries: LogEntry[] = [];
  private children: ChildProcess[] = [];
  private files = new Map<string, { position: number; inode: number; remainder: string; polling: boolean }>();
  private timer?: ReturnType<typeof setInterval>;
  private retries = new Set<ReturnType<typeof setTimeout>>();
  private stopped = false;
  private seen = new Set<string>();
  push(source: LogEntry['source'], text: string) {
    if (!text.trim()) return;
    const fingerprint = `${source}:${text}`;
    if (this.seen.has(fingerprint)) return;
    this.seen.add(fingerprint);
    if (this.seen.size > 1600) this.seen.delete(this.seen.values().next().value!);
    const level = /\b(ERROR|FATAL|FAIL|SEVERE)\b/i.test(text) ? 'ERROR' : /\bWARN(?:ING)?\b/i.test(text) ? 'WARN' : 'INFO';
    const iso = text.match(/\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)/)?.[0];
    const phpTime = text.match(/^\[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)\]/)?.[1];
    const at = iso ?? (phpTime ? phpTime.replace(' ', 'T') + 'Z' : new Date().toISOString());
    this.entries.push({ id: randomUUID(), source, level, at, text: text.slice(0, 6000) });
    if (this.entries.length > 800) this.entries.splice(0, this.entries.length - 800);
  }
  async readLocal(source: LogEntry['source'], path: string) {
    let state = this.files.get(path);
    if (!state) { state = { position: -1, inode: 0, remainder: '', polling: false }; this.files.set(path, state); }
    if (state.polling) return;
    state.polling = true;
    try {
      const s = await stat(path);
      if (state.position < 0 || s.ino !== state.inode || s.size < state.position) {
        state.position = Math.max(0, s.size - 128_000); state.inode = s.ino; state.remainder = '';
      }
      if (s.size === state.position) return;
      const file = await open(path, 'r');
      try {
        const size = Math.min(s.size - state.position, 256_000);
        const buffer = Buffer.alloc(size);
        const { bytesRead } = await file.read(buffer, 0, size, state.position);
        state.position += bytesRead;
        const lines = (state.remainder + buffer.subarray(0, bytesRead).toString()).split('\n');
        state.remainder = lines.pop() ?? '';
        for (const line of lines) this.push(source, line);
      } finally { await file.close(); }
    } catch { /* 服务尚未启动或日志尚未生成，健康状态由 HTTP 采集给出。 */ }
    finally { state.polling = false; }
  }
  start() {
    const localFiles: [LogEntry['source'], string][] = [];
    if (config.mode === 'native') localFiles.push(['PHP', config.phpLog]);
    if (config.javaMode === 'native') localFiles.push(['Java', config.javaLog]);
    const readFiles = () => { for (const [source, path] of localFiles) void this.readLocal(source, path); };
    if (localFiles.length) { readFiles(); this.timer = setInterval(readFiles, 1000); }
    const sources: [string, LogEntry['source']][] = [['mq-order-lab-kafka-1', 'Kafka']];
    if (config.javaMode === 'docker') sources.push(['mq-order-lab-java-review-1', 'Java']);
    if (config.mode === 'docker') sources.push(['mq-order-lab-php-web-1', 'PHP'], ['mq-order-lab-php-outbox-1', 'PHP'], ['mq-order-lab-php-results-1', 'PHP']);
    for (const [container, source] of sources) this.follow(container, source);
  }
  private follow(container: string, source: LogEntry['source']) {
    const child = spawn('docker', ['logs', '--timestamps', '--follow', '--tail', '70', container], { stdio: ['ignore', 'pipe', 'pipe'] });
    this.children.push(child);
    for (const stream of [child.stdout, child.stderr]) {
      let pending = '';
      stream?.on('data', (buffer: Buffer) => {
        const lines = (pending + buffer.toString()).split('\n'); pending = lines.pop() ?? '';
        for (const line of lines) this.push(source, line);
      });
    }
    child.on('error', error => this.push(source, `日志采集不可用：${error.message}`));
    // 停止的容器恢复后重新连接；按含时间戳的原文去重，避免重复追加历史行。
    child.on('close', () => {
      this.children = this.children.filter(c => c !== child);
      if (this.stopped) return;
      const timer = setTimeout(() => { this.retries.delete(timer); this.follow(container, source); }, 5000);
      this.retries.add(timer);
    });
  }
  stop() { this.stopped = true; if (this.timer) clearInterval(this.timer); for (const timer of this.retries) clearTimeout(timer); for (const child of this.children) child.kill(); }
}
