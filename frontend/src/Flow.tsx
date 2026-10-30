import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { ReactFlow, Handle, Position, MarkerType, useReactFlow, useNodes, useNodesInitialized, type NodeProps, type Node, type Edge } from '@xyflow/react';
import { CheckIcon, ChevronRightIcon } from '@radix-ui/react-icons';
import type { Step } from '../shared/types';

type FlowData = { title: string; subtitle: string; step?: Step; current: boolean; inPosition: Position; outPosition: Position; statusLabel: string; committed: boolean; inspect: (step: Step) => void };
type LabNode = Node<FlowData>;
function ServiceNode({ data }: NodeProps<LabNode>) {
  return <div className={`service-node ${data.step ? 'done' : ''} ${data.current ? 'current' : ''}`}>
    <Handle type="target" position={data.inPosition}/>
    <button type="button" className="node-button nodrag" disabled={!data.step} onClick={() => { if (data.step) data.inspect(data.step); }} aria-label={`${data.subtitle}：${data.title}${data.step ? '，查看处理记录' : '，等待处理记录'}`}>
      <span className="node-meta">{data.subtitle}{data.step && <CheckIcon/>}</span>
      <strong>{data.title}</strong>
      <span className="node-bottom"><span className="node-state">{data.step ? data.statusLabel : '等待处理记录'}</span>{data.step && <ChevronRightIcon/>}</span>
      {data.committed && <span className="node-commit">消费进度已提交</span>}
    </button>
    <Handle type="source" position={data.outPosition}/>
  </div>;
}
const nodeTypes = { service: ServiceNode };
function FitDiagram({ width }: { width: number }) {
  const { fitView } = useReactFlow();
  const initialized = useNodesInitialized();
  const layout = useNodes().map(n => `${n.id}:${n.position.x}:${n.position.y}:${n.measured?.width}:${n.measured?.height}`).join('|');
  useEffect(() => {
    if (!initialized) return;
    const frame = requestAnimationFrame(() => void fitView({ padding: 0.04, maxZoom: 1, duration: 0 }));
    return () => cancelAnimationFrame(frame);
  }, [width, initialized, layout, fitView]);
  return null;
}
export function Flow({ steps, onStep }: { steps: Step[]; onStep: (step: Step) => void }) {
  const container = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  useLayoutEffect(() => {
    const observer = new ResizeObserver(entries => setWidth(Math.round(entries[0].contentRect.width)));
    if (container.current) { setWidth(container.current.clientWidth); observer.observe(container.current); }
    return () => observer.disconnect();
  }, []);
  const compact = width > 0 && width < 550;
  const nodeWidth = compact ? Math.min(310, width - 40) : Math.min(230, (width - 80) / 3);
  const gap = compact ? 0 : (width - 32 - nodeWidth * 3) / 2;
  const x = (column: number) => 16 + column * (nodeWidth + gap);
  const has = (n: number) => steps.some(s => s.index === n);
  const config: [string, string, number, number, number, Position, Position][] = [
    ['保存订单', 'PHP · 订单服务', 0, x(0), 0, Position.Left, Position.Right],
    ['订单创建消息', 'Kafka · 创建 Topic', 1, x(1), 0, Position.Left, Position.Right],
    ['审核订单', 'Java · 审核服务', 2, x(2), 0, Position.Left, Position.Bottom],
    ['审核结果消息', 'Kafka · 结果 Topic', 4, x(2), 126, Position.Top, Position.Left],
    ['更新订单结果', 'PHP · 结果消费者', 5, x(1), 126, Position.Right, Position.Left],
  ];
  const latest = steps.at(-1)?.index ?? 0;
  const activeStage = latest >= 5 ? 5 : latest === 4 ? 4 : latest >= 2 ? 2 : latest;
  const labels = ['订单已落库', 'Kafka 已确认', '审核已完成', 'Kafka 已确认', '结果已落库'];
  const nodes: LabNode[] = config.map(([title, subtitle, stage, left, top, inPosition, outPosition], i) => {
    const step = steps.find(s => s.index === stage);
    const position = (stage === 1 || stage === 4) && step?.partition !== undefined && step.offset !== undefined
      ? `P${step.partition} · Offset ${step.offset}` : labels[i];
    return {
    id: String(i), type: 'service', style: { width: nodeWidth },
    position: compact ? { x: 0, y: i * 124 } : { x: left, y: top },
    data: { title, subtitle, step, current: stage === activeStage && has(stage), inPosition: compact ? Position.Top : inPosition, outPosition: compact ? Position.Bottom : outPosition, statusLabel: position, committed: stage === 2 ? has(3) : stage === 5 ? has(6) : false, inspect: onStep },
  }; });
  const edges: Edge[] = [0, 1, 2, 3].map(i => ({ id: `e${i}`, source: String(i), target: String(i + 1), type: 'smoothstep', className: has(config[i + 1][2]) ? 'edge-done' : '', markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 } }));
  return <div className="flow-wrap" ref={container} data-compact={compact} aria-label="PHP 到 Java 再回到 PHP 的消息流转图">
    {width > 0 && <ReactFlow key={String(compact)} nodes={nodes} edges={edges} nodeTypes={nodeTypes} fitView fitViewOptions={{ padding: 0.04, maxZoom: 1 }} nodesDraggable={false} nodesConnectable={false} nodesFocusable={false} edgesFocusable={false} panOnDrag={false} zoomOnScroll={false} zoomOnDoubleClick={false} zoomOnPinch={false} preventScrolling={false} minZoom={0.8} proOptions={{ hideAttribution: false }}><FitDiagram width={width}/></ReactFlow>}
    {width > 0 && !compact && <div className="flow-caption">Topic 分别存储<br/>Offset 独立编号</div>}
  </div>;
}
