import { useEffect, useState } from 'react';
import { Link, Outlet } from '@tanstack/react-router';
import { Accordion, Button, Card, Chip, Drawer, Form, Input, Label, ListBox, TextField } from '@heroui/react';
import { ActivityLogIcon, ArrowRightIcon, CheckIcon, ChevronRightIcon, Cross2Icon, LayersIcon, PlayIcon, PlusIcon, ReloadIcon } from '@radix-ui/react-icons';
import { useLab } from './store';
import { Flow } from './Flow';
import { Partitions } from './Partitions';
import { Logs } from './Logs';
import { Playback } from './Playback';
import { CopyButton } from './CopyButton';
import { dateLabel, money, STATUS, stepsFor } from '../shared/model';
import type { Order, RecordEntry, Step } from '../shared/types';

type Inspection = { title: string; description: string; value: unknown; summary?: string; facts?: { label: string; value: string }[] };
const timeKind = (step: Step) => step.source.includes('查询') || step.source.includes('采集时') ? '查询于' : '记录于';
function recordInspection(record: RecordEntry): Inspection {
  return { title: `${record.event?.eventType ?? '消息'} · P${record.partition} / ${record.offset}`, description: `${record.topic} · Kafka 实际记录`, summary: '这条记录已经保存在 Kafka 中。消费者读取、提交进度后，它的 Offset 保持原样。', facts: [
    { label: 'Topic', value: record.topic }, { label: '分区 / Offset', value: `Partition ${record.partition} / ${record.offset}` },
    { label: 'Key（订单编号）', value: record.key || '无 Key' }, { label: '事件编号', value: record.event?.eventId ?? '原文不是事件 JSON' }, { label: '记录时间', value: dateLabel(record.timestamp) },
  ], value: record };
}
export function Layout() {
  const snapshot = useLab(s => s.snapshot); const refresh = useLab(s => s.refresh); const error = useLab(s => s.error);
  useEffect(() => { void refresh(); const timer = setInterval(() => void refresh(), 2000); return () => clearInterval(timer); }, [refresh]);
  return <div className="app-shell">
    <header className="app-header"><Link to="/" className="brand"><span className="brand-symbol"><LayersIcon/></span><strong>Kafka 订单实验室</strong></Link><nav aria-label="主导航"><Link to="/" activeProps={{ className: 'active' }} activeOptions={{ exact: true }}><ActivityLogIcon/>订单流转</Link><Link to="/partitions" activeProps={{ className: 'active' }}><LayersIcon/>分区与进度</Link><Link to="/logs" activeProps={{ className: 'active' }}>运行日志</Link></nav><span className="local-label">本机实验环境</span></header>
    <div className="status-strip"><div className="service-health">{(['php', 'java', 'kafka'] as const).map(key => {
      const service = snapshot?.health[key];
      const unknown = Boolean(error) || !service;
      return <details key={key}>
        <summary><i className={unknown ? 'unknown' : service.ok ? 'online' : 'offline'}/>{key === 'java' ? 'Java' : key.toUpperCase()}<span>{unknown ? '状态未获取' : service.ok ? '已连接' : '未连接'}</span></summary>
        <div>{service?.endpoint}<p>{unknown ? '观察服务尚未返回最新状态，不能据此判断业务服务是否已启动。' : service.error ?? `最近成功采集 ${dateLabel(service.checkedAt)}`}</p></div>
      </details>;
    })}</div><span>{snapshot?.mode === 'docker' ? 'Docker PHP' : '本机 PHP'} · 每 2 秒采集 <span className="last-updated">· {dateLabel(snapshot?.capturedAt)}</span></span></div>
    {error && <div className="connection-error" role="alert">{error} 保留上次显示的数据。</div>}
    <Outlet/>
    <footer className="app-footer"><span>业务数据来自真实服务 · 观察消费者使用独立消费组</span></footer>
  </div>;
}

function OrderSidebar() {
  const { snapshot, selected, select, form, setForm, submit, busy, feedback, feedbackKind, mode } = useLab();
  return <aside className="order-sidebar">
    <div className="sidebar-title"><h2>创建订单</h2><span>总额 ≤ ¥1,000 通过</span></div>
    <Form onSubmit={e => { e.preventDefault(); void submit(); }} className="create-form" aria-busy={busy}>
      <fieldset disabled={busy}>
      <TextField name="product" value={form.product} onChange={product => setForm({ product })} isRequired isDisabled={busy} fullWidth><Label>商品名称</Label><Input maxLength={100}/></TextField>
      <div className="form-pair"><TextField name="quantity" value={form.quantity} onChange={quantity => setForm({ quantity })} isRequired isDisabled={busy} fullWidth><Label>数量</Label><Input type="number" min="1" max="1000" step="1"/></TextField><TextField name="price" value={form.price} onChange={price => setForm({ price })} isRequired isDisabled={busy} fullWidth><Label>单价 / 元</Label><Input type="number" min="0.01" max="100000" step="0.01"/></TextField></div>
      <div className="order-total"><span>订单总额</span><strong>{money(Math.round(Number(form.price) * 100) * Number(form.quantity))}</strong></div>
      </fieldset>
      <Button type="submit" fullWidth isPending={busy} isDisabled={busy || mode === 'replay'} className="create-button"><PlusIcon/>{busy ? '正在提交…' : '创建订单并观察'}</Button>
      <Button type="button" variant="ghost" size="sm" fullWidth isDisabled={busy} onPress={() => setForm({ product: '显示器', quantity: '1', price: '1200.00' })}>试试审核拒绝的订单 <ChevronRightIcon/></Button>
      <p className="form-hint">{mode === 'replay' ? '当前正在回放，切回实时观察后可创建订单。' : '订单保存后，由 Java 异步审核。'}</p>
      {feedback && <p className={`form-feedback ${feedbackKind}`} role={feedbackKind === 'error' ? 'alert' : 'status'}>{feedback}</p>}
    </Form>
    <div className="sidebar-title recent-title"><h2>最近订单</h2><span>{snapshot?.orders.length ?? 0}<span className="hidden max-[700px]:inline"> · 左右滑动</span></span></div>
    <ListBox aria-label="最近订单" selectionMode="single" disallowEmptySelection selectedKeys={new Set(selected ? [selected] : [])} onSelectionChange={keys => { const id = [...keys][0]; if (id) select(String(id)); }} className="order-list">{snapshot?.orders.map(order => <ListBox.Item key={order.orderId} id={order.orderId} textValue={`${order.product}，${money(order.totalCents)}，${STATUS[order.status] ?? order.status}`} className="recent-order"><span className="order-item-top"><strong>{order.product}</strong><span>{money(order.totalCents)}</span></span><span className="order-item-bottom"><code>…{order.orderId.slice(-8)}</code><Chip className="text-[13px]" size="sm" variant="soft" color={order.status === 'CONFIRMED' ? 'success' : order.status === 'REJECTED' ? 'danger' : 'warning'}>{STATUS[order.status] ?? order.status}</Chip></span></ListBox.Item>)}</ListBox>{!snapshot?.orders.length && <p className="muted empty-orders">创建订单后，它会出现在这里。</p>}

  </aside>;
}

function useViewedOrder() {
  const snapshot = useLab(s => s.snapshot); const selected = useLab(s => s.selected);
  const mode = useLab(s => s.mode); const cursor = useLab(s => s.cursor); const replayFrames = useLab(s => s.replayFrames);
  const frame = mode === 'replay' ? replayFrames[cursor] : undefined;
  const order = mode === 'replay' ? frame?.order : snapshot?.orders.find(o => o.orderId === selected);
  const topics = mode === 'replay' ? frame?.topics ?? [] : snapshot?.topics ?? [];
  const steps = frame ? replayFrames.slice(0, cursor + 1).map(f => f.step) : order ? stepsFor(order, topics, snapshot?.health.kafka.checkedAt ?? snapshot?.capturedAt ?? '') : [];
  return { snapshot, mode, frame, order, topics, steps };
}

export function Home() {
  const { snapshot, mode, frame, order, topics, steps } = useViewedOrder();
  const [inspection, setInspection] = useState<Inspection | null>(null);
  const [inspectionOpen, setInspectionOpen] = useState(false);
  const inspect = (value: Inspection) => { setInspection(value); setInspectionOpen(true); };
  const refresh = useLab(s => s.refresh);
  const inspectRecord = (record: RecordEntry) => inspect(recordInspection(record));
  const inspectStep = (step: Step) => inspect({ title: step?.title ?? '等待处理记录', description: step?.source ?? '这一环节尚未采集到处理记录。', summary: step?.detail, facts: step ? [
    { label: timeKind(step), value: dateLabel(step.at) }, { label: '订单编号', value: order?.orderId ?? '—' },
    ...(step.partition !== undefined ? [{ label: '分区', value: `Partition ${step.partition}` }] : []),
    ...(step.offset !== undefined ? [{ label: step.index === 3 || step.index === 6 ? '已提交下一位置' : '消息 Offset', value: step.offset }] : []),
  ] : [], value: step ? { step, order } : { orderId: order?.orderId } });
  return <div className="workspace grid grid-cols-[256px_minmax(0,1fr)] max-[1200px]:grid-cols-[236px_minmax(0,1fr)] max-[700px]:flex max-[700px]:flex-col"><OrderSidebar/><main className="workspace-main">
    <div className="workspace-heading"><div><h1>订单流转</h1></div><Playback/></div>
    {mode === 'replay' && <div className="replay-notice"><PlayIcon/><span>回看「{order?.product}」· {frame?.step.title} · 采集于 {dateLabel(frame?.observedAt)}。仅展示当时的证据，不重发消息。</span></div>}
    {snapshot && !snapshot.health.php.ok && <p className="inline-warning">PHP 暂时无法连接。保留的订单信息最近成功采集于 {dateLabel(snapshot.health.php.checkedAt)}。</p>}
    {snapshot && !snapshot.health.java.ok && <p className="inline-warning">Java 审核服务暂未连接。待审核的订单会保留，服务恢复后继续处理。</p>}
    {order ? <>
      <OrderSummary order={order} replay={mode === 'replay'}/>
      <Card className="journey-section gap-2"><Card.Header className="section-heading"><Card.Title className="text-base font-semibold" render={props => <h2 {...props}/>}>消息处理路径</Card.Title><span className="flow-legend"><i/>已有证据<span className="wait-dot"/>等待处理</span></Card.Header>
        <Flow steps={steps} onStep={inspectStep}/>
        <div className="current-step" aria-live="polite"><span className="step-indicator">{[2, 3, 4].includes(steps.at(-1)?.index ?? 0) ? 'Java' : 'PHP'}</span><div><strong>{steps.at(-1)?.title}</strong><p>{steps.at(-1)?.detail}</p></div><Button variant="ghost" size="sm" onPress={() => { const step = steps.at(-1); if (step) inspectStep(step); }}>本步详情 <ChevronRightIcon/></Button></div>
        <Accordion className="step-evidence"><Accordion.Item id="evidence"><Accordion.Heading><Accordion.Trigger>全部处理记录（{steps.length}）<Accordion.Indicator/></Accordion.Trigger></Accordion.Heading><Accordion.Panel><Accordion.Body><p className="timeline-note">按业务步骤排列。记录时间来自业务数据，查询时间表示最近采集时间。</p><ol>{steps.map(s => <li key={s.id}><CheckIcon/><button onClick={() => inspectStep(s)}>{s.title}</button><span>{s.source}</span><time>{timeKind(s)} {dateLabel(s.at)}</time></li>)}</ol></Accordion.Body></Accordion.Panel></Accordion.Item></Accordion>
      </Card>
      <Partitions topics={topics} order={order} replay={mode === 'replay'} stale={!snapshot?.health.kafka.ok} onRecord={inspectRecord}/>
      {mode === 'live' && <Accordion className="logs-disclosure"><Accordion.Item id="logs"><Accordion.Heading><Accordion.Trigger><span className="flex items-center gap-2"><ActivityLogIcon/>查看三端运行日志</span><Accordion.Indicator/></Accordion.Trigger></Accordion.Heading><Accordion.Panel><Accordion.Body><Logs compact/></Accordion.Body></Accordion.Panel></Accordion.Item></Accordion>}
    </> : <div className="welcome-state"><LayersIcon/><h2>{snapshot?.orders.length ? '正在读取这笔订单' : '先创建一笔订单'}</h2><p>{snapshot?.orders.length ? '订单已保存，采集到处理记录后会显示在这里。' : '订单保存后，这里会显示它的消息路径、分区与消费进度。'}</p><Button variant="outline" onPress={() => void refresh()}><ReloadIcon/>重新连接服务</Button></div>}
    {inspection && <Inspector inspection={inspection} isOpen={inspectionOpen} onClose={() => setInspectionOpen(false)}/>}
  </main></div>;
}

function OrderSummary({ order, replay }: { order: Order; replay: boolean }) {
  return <Card className="summary-card" key={order.orderId}><Card.Content className="order-summary"><div><div className="summary-title"><h2>{order.product} <span>× {order.quantity}</span></h2><Chip className="text-[13px]" size="sm" color={order.status === 'CONFIRMED' ? 'success' : order.status === 'REJECTED' ? 'danger' : 'warning'} variant="soft">{order.status === 'PENDING_REVIEW' ? <span className="pending-dot"/> : order.status === 'REJECTED' ? <Cross2Icon/> : <CheckIcon/>}{replay ? '当时：' : ''}{STATUS[order.status] ?? order.status}</Chip></div><div className="summary-details"><CopyButton value={order.orderId} label="复制订单编号" className="order-id"><code>{order.orderId}</code></CopyButton>{order.reason && <p className="summary-reason">{order.reason}</p>}</div></div><div className="summary-amount"><strong>{money(order.totalCents)}</strong><span>{dateLabel(order.createdAt)} 创建</span></div></Card.Content></Card>;
}

function Inspector({ inspection, isOpen, onClose }: { inspection: Inspection; isOpen: boolean; onClose: () => void }) {
  return <Drawer isOpen={isOpen} onOpenChange={open => { if (!open) onClose(); }}>
    <Drawer.Backdrop isDismissable>
      <Drawer.Content placement="right">
        <Drawer.Dialog className="inspection-drawer">
          <Drawer.Header><div className="flex items-center justify-between gap-4"><span className="muted text-sm">处理详情</span><Drawer.CloseTrigger aria-label="关闭详情"/></div><Drawer.Heading>{inspection.title}</Drawer.Heading><p className="muted text-sm break-words">{inspection.description}</p></Drawer.Header>
          <Drawer.Body>{inspection.summary && <p className="inspection-summary">{inspection.summary}</p>}{inspection.facts && <dl className="inspection-facts">{inspection.facts.map(f => <div key={f.label}><dt>{f.label}</dt><dd>{f.value}</dd></div>)}</dl>}<Accordion><Accordion.Item id="json"><Accordion.Heading><Accordion.Trigger>查看原始 JSON<Accordion.Indicator/></Accordion.Trigger></Accordion.Heading><Accordion.Panel><Accordion.Body><pre className="inspection-json">{JSON.stringify(inspection.value, null, 2)}</pre></Accordion.Body></Accordion.Panel></Accordion.Item></Accordion></Drawer.Body>
        </Drawer.Dialog>
      </Drawer.Content>
    </Drawer.Backdrop>
  </Drawer>;
}

export function PartitionPage() {
  const { snapshot, mode, order, topics } = useViewedOrder(); const [inspection, setInspection] = useState<Inspection | null>(null);
  const [inspectionOpen, setInspectionOpen] = useState(false);
  const inspect = (value: Inspection) => { setInspection(value); setInspectionOpen(true); };
  return <main className="standalone-page"><div className="workspace-heading"><div><h1>消息位置与消费进度</h1></div><Playback/></div><p className="page-intro">每个分区独立追加记录。消息的 Offset 固定，消费者提交的下一位置随处理向前移动。</p><Partitions topics={topics} order={order} replay={mode === 'replay'} stale={!snapshot?.health.kafka.ok} onRecord={r => inspect(recordInspection(r))}/><div className="concept-notes"><article><strong>消息 Offset</strong><p>一条记录在某个分区里的位置。处理完成不会删除或重新编号。</p></article><article><strong>已提交的下一位置</strong><p>例如处理完 Offset 3，通常提交 4，表示恢复时从位置 4 继续。</p></article><article><strong>位置差与重读</strong><p>位置差是日志末端减去提交位置。重读移动的是消费进度，数据库不会自动回到过去。</p></article></div>{inspection && <Inspector inspection={inspection} isOpen={inspectionOpen} onClose={() => setInspectionOpen(false)}/>}</main>;
}
export function LogsPage() { return <main className="standalone-page"><div className="workspace-heading"><div><h1>从日志核对处理过程</h1></div><Link to="/" className="back-link">返回订单 <ArrowRightIcon/></Link></div><p className="page-intro">业务发送确认来自 PHP / Java；Kafka Broker 日志用于查看服务运行和连接情况。</p><Logs/></main>; }
