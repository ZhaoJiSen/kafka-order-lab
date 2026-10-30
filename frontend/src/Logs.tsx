import { useState } from 'react';
import { Button, Card, Checkbox, Description, Input, Label, TextField, ToggleButton, ToggleButtonGroup } from '@heroui/react';
import { PauseIcon, PlayIcon } from '@radix-ui/react-icons';
import type { LogEntry } from '../shared/types';
import { dateLabel } from '../shared/model';
import { useLab } from './store';
import { LabSelect } from './LabSelect';
import { CopyButton } from './CopyButton';

export function Logs({ compact = false }: { compact?: boolean }) {
  const logs = useLab(s => s.logs); const mode = useLab(s => s.mode); const orderId = useLab(s => s.selected);
  const [source, setSource] = useState('全部'); const [level, setLevel] = useState('全部');
  const [query, setQuery] = useState(''); const [onlyOrder, setOnlyOrder] = useState(false);
  const [paused, setPaused] = useState<LogEntry[] | null>(null);
  const entries = (paused ?? logs).filter(l => (source === '全部' || l.source === source) && (level === '全部' || l.level === level) && l.text.toLowerCase().includes(query.toLowerCase()) && (!onlyOrder || l.text.includes(orderId)));
  const visible = entries.sort((a, b) => Date.parse(b.at) - Date.parse(a.at)).slice(0, compact ? 5 : 150);
  return <Card className={`logs-section ${compact ? 'compact' : ''}`}>
    <div className="section-heading"><div><h2>运行日志 <span className="subtle-label">Asia/Shanghai</span></h2></div><Button variant="outline" className="log-pause" size="sm" aria-pressed={paused !== null} onPress={() => setPaused(paused ? null : [...logs])}>{paused ? <PlayIcon/> : <PauseIcon/>}{paused ? '继续更新' : '暂停更新'}</Button></div>
    {!compact && <div className="log-context"><span>{orderId ? <>当前订单 <code>{orderId}</code></> : '尚未选择订单'}</span><span>{paused ? '画面已暂停' : '日志实时更新'}{mode === 'replay' ? ' · 不参与教学回放' : ''}</span></div>}
    <div className="log-toolbar"><ToggleButtonGroup size="sm" selectionMode="single" disallowEmptySelection aria-label="日志来源" selectedKeys={new Set([source])} onSelectionChange={keys => { const key = [...keys][0]; if (key) setSource(String(key)); }}>{['全部', 'PHP', 'Java', 'Kafka'].map(s => <ToggleButton key={s} id={s}>{s}</ToggleButton>)}</ToggleButtonGroup><TextField aria-label="搜索日志" value={query} onChange={setQuery} className="log-search"><Input placeholder="搜索日志内容…"/></TextField><LabSelect label="日志级别" value={level} onChange={setLevel} options={['全部', 'INFO', 'WARN', 'ERROR'].map(value => ({value, label: value === '全部' ? '全部级别' : value}))} className="level-select"/></div>
    {!compact && <Checkbox isSelected={onlyOrder} onChange={setOnlyOrder} isDisabled={!orderId}><Checkbox.Content><Checkbox.Control><Checkbox.Indicator/></Checkbox.Control><Label>只看当前订单</Label></Checkbox.Content><Description>按订单编号匹配，Broker 系统日志通常不包含订单编号。</Description></Checkbox>}
    <div className="log-entries" aria-label="日志记录">{visible.length ? visible.map(l => <div className="log-entry" key={l.id}><time>{dateLabel(l.at)}</time><span className={`log-source ${l.source.toLowerCase()}`}>{l.source}</span><span className={`log-level ${l.level.toLowerCase()}`}>{l.level}</span><pre>{l.text}</pre><CopyButton value={l.text} label="复制这条日志" className="copy-log"/></div>) : <div className="empty-state"><p>{logs.length ? '没有符合筛选条件的日志。' : '尚未采集到日志，服务运行后会自动更新。'}</p>{(query || source !== '全部' || level !== '全部' || onlyOrder) && <Button variant="outline" size="sm" onPress={() => { setQuery(''); setSource('全部'); setLevel('全部'); setOnlyOrder(false); }}>清除筛选</Button>}</div>}</div>
    <p className="log-note">Kafka 栏是 Broker 的运行日志；每笔订单的写入确认由 PHP / Java 客户端记录。按记录时间倒序展示。最近保留 800 条，当前显示 {visible.length} 条。{paused ? '已暂停画面更新，后台仍在采集。' : ''}</p>
  </Card>;
}
