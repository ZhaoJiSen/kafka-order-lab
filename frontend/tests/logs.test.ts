import { test } from 'node:test';
import assert from 'node:assert/strict';
import { appendFile, mkdtemp, rename, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Logs } from '../server/logs';

test('本机 PHP 与 Java 日志分别记录读取位置，保留半行并避免重复读取', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'order-lab-logs-'));
  try {
    const php = join(dir, 'php.log'); const java = join(dir, 'java.log');
    await writeFile(php, '[2026-10-27 08:00:00] local.INFO: PHP 已发送\n');
    await writeFile(java, '2026-10-27T16:00:01.000+08:00 INFO Java 已审核\n下一');
    const logs = new Logs();
    await Promise.all([logs.readLocal('PHP', php), logs.readLocal('Java', java)]);
    assert.equal(logs.entries.filter(e => e.source === 'PHP').length, 1);
    assert.equal(logs.entries.filter(e => e.source === 'Java').length, 1);
    assert.equal(logs.entries.find(e => e.source === 'Java')?.at, '2026-10-27T16:00:01.000+08:00');
    await appendFile(java, '条记录\n');
    await logs.readLocal('Java', java);
    await logs.readLocal('PHP', php);
    assert.equal(logs.entries.length, 3);
    assert.equal(logs.entries[2].text, '下一条记录');
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test('本机日志尚未创建、被截断或轮转后仍能继续采集', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'order-lab-log-rotation-'));
  try {
    const path = join(dir, 'java.log'); const logs = new Logs();
    await logs.readLocal('Java', path);
    await writeFile(path, '较长的第一条记录\n');
    await logs.readLocal('Java', path);
    await writeFile(path, '新记录\n');
    await logs.readLocal('Java', path);
    await rename(path, `${path}.1`);
    await writeFile(path, '轮转后记录\n');
    await logs.readLocal('Java', path);
    assert.deepEqual(logs.entries.map(e => e.text), ['较长的第一条记录', '新记录', '轮转后记录']);
  } finally { await rm(dir, { recursive: true, force: true }); }
});
