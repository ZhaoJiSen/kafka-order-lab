import { useEffect, useRef, useState } from 'react';
import { Card, Tabs } from '@heroui/react';
import { ArrowRightIcon } from '@radix-ui/react-icons';
import type { Order, RecordEntry, Topic } from '../shared/types';
import { partitionForKey } from '../shared/model';

export function Partitions({ topics, order, replay, stale, onRecord }: { topics: Topic[]; order?: Order; replay: boolean; stale: boolean; onRecord: (r: RecordEntry) => void }) {
  const [tab, setTab] = useState(0);
  const tracks = useRef(new Map<number, HTMLDivElement>());
  const topic = topics[tab];
  const version = topic?.partitions.map(p => `${p.id}:${p.records.length}:${p.records.at(-1)?.offset}`).join(',');
  useEffect(() => {
    const center = () => {
      for (const track of tracks.current.values()) {
        const record = track.querySelector<HTMLElement>('.record.selected');
        if (!record) continue;
        const left = record.getBoundingClientRect().left - track.getBoundingClientRect().left + track.scrollLeft - (track.clientWidth - record.offsetWidth) / 2;
        track.scrollTo({ left, behavior: 'instant' });
      }
    };
    center();
    const resize = new ResizeObserver(center);
    for (const track of tracks.current.values()) resize.observe(track);
    return () => resize.disconnect();
  }, [order?.orderId, tab, version]);
  const selectedRecord = topic?.partitions.flatMap(p => p.records).find(r => r.key === order?.orderId);
  const calculation = order && topic ? partitionForKey(order.orderId, topic.partitions.length) : null;
  return <Card className="partition-section">
    <div className="section-heading"><div><h2>分区中的消息</h2></div><span className="muted">{topic?.partitions.length ?? '—'} 个分区 · 每行独立编号</span></div>
    <Tabs selectedKey={String(tab)} onSelectionChange={key => setTab(Number(key))} variant="secondary" className="topic-selector">
      <Tabs.ListContainer><Tabs.List aria-label="选择 Topic">{['订单创建 · PHP → Java', '审核结果 · Java → PHP'].map((text, index) => <Tabs.Tab key={index} id={String(index)} aria-label={text}><span className="topic-tab-copy"><span>{index === 0 ? '订单创建' : '审核结果'}<span className="topic-direction">{index === 0 ? 'PHP → Java' : 'Java → PHP'}</span></span><code>{index === 0 ? 'demo.orders.created' : 'demo.orders.reviewed'}</code></span><Tabs.Indicator/></Tabs.Tab>)}</Tabs.List></Tabs.ListContainer>
      <Tabs.Panel id={String(tab)}>
      <code className="selected-topic-name">{topic?.name}</code>
    {!topic ? <div className="empty-state">等待 Kafka 分区信息。检查 Kafka 容器是否正在运行。</div> : <>
      {stale && <p className="inline-warning">Kafka 暂不可用，以下保留上次采集的记录与进度。</p>}
      <div className="partition-legend"><span><i className="record-dot"/> 当前订单的记录</span><span><i className="cursor-dot"/> 消费组已提交的下一位置</span><span>末端 = 下一写入位置</span></div>
      <div className="partition-list">{topic.partitions.map(p => {
        const contains = p.records.some(r => r.key === order?.orderId);
        return <div key={p.id} className={`partition-row ${contains ? 'contains-order' : ''}`}>
          <div className="partition-label"><strong>P{p.id}</strong><span>分区</span></div>
          <div className="record-track">
            <div className="records" ref={element => { if (element) tracks.current.set(p.id, element); else tracks.current.delete(p.id); }}>{p.records.length > 0 ? p.records.map(r => <button key={r.offset} onClick={() => onRecord(r)} className={`record ${r.key === order?.orderId ? 'selected' : ''} ${p.committed === r.offset ? 'has-cursor' : ''}`} title={`Offset ${r.offset} · ${r.key}`} aria-label={`分区 ${p.id}，Offset ${r.offset}，${r.key === order?.orderId ? '当前订单' : r.key}`}><span>{r.offset}</span><small>{r.key === order?.orderId ? '本单' : r.key.slice(-5) || '无 Key'}</small></button>) : <span className="track-empty">{p.end === p.low ? '此分区暂无记录' : replay ? '本步骤尚未展示记录' : '正在读取最近记录…'}</span>}<span className={`end-marker ${p.committed === p.end ? 'has-cursor' : ''}`} title="分区日志末端，下一条记录写入的位置"><ArrowRightIcon/><b>{p.end}</b><small>下一写入</small></span></div>
            <div className="cursor-line"><span className="consumer-pointer">▲</span>{p.committed === null ? (replay ? '本步骤未展示消费进度' : '尚未提交 / 暂无进度') : <>已提交下一位置 <b>{p.committed}</b></>}<span className="range">显示 {p.records[0]?.offset ?? '—'}–{p.records.at(-1)?.offset ?? '—'} · 最多 100 条</span></div>
          </div>
          <div className="lag"><strong>{p.lag ?? '—'}</strong><span>位置差</span></div>
        </div>;
      })}</div>
      <div className="partition-explainer">{tab === 0 && calculation ? <><span className="explain-mark">Key → P{selectedRecord?.partition ?? calculation.partition}</span><p>PHP 以订单编号为 Key：<code>CRC32(Key) = {calculation.hash}</code>，<code>{calculation.hash} % {topic.partitions.length} = {calculation.partition}</code>。{selectedRecord ? `实际记录确认写入分区 ${selectedRecord.partition}，Offset ${selectedRecord.offset}。` : '这是当前规则下的预计分区，实际位置等待 Kafka 确认。'}</p></> : <><span className="explain-mark">独立编号</span><p>Java 客户端独立选择结果消息的分区。{selectedRecord ? `本条结果实际写入分区 ${selectedRecord.partition}，Offset ${selectedRecord.offset}。` : '结果写入后，这里显示实际分区与 Offset。'}同一订单在两个 Topic 的分区编号可以不同。</p></>}</div>
      <div className="group-footer"><span>消费组 <code>{topic.groupId}</code></span><span>{topic.members} 个活跃实例 · {topic.groupState}</span></div>
    </>}
      </Tabs.Panel>
    </Tabs>
  </Card>;
}
