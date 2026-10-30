import { useEffect } from 'react';
import { Button, ToggleButton, ToggleButtonGroup } from '@heroui/react';
import { ChevronLeftIcon, ChevronRightIcon, PauseIcon, PlayIcon, ReloadIcon } from '@radix-ui/react-icons';
import { LabSelect } from './LabSelect';
import { useLab } from './store';

export function Playback() {
  const { mode, replay, live, frames, replayFrames, cursor, move, playing, setPlaying, speed, setSpeed, historyLoading, historyError } = useLab();
  useEffect(() => {
    if (!playing || mode !== 'replay') return;
    const timer = setTimeout(() => {
      if (cursor >= replayFrames.length - 1) setPlaying(false);
      else move(cursor + 1);
    }, 1800 / speed);
    return () => clearTimeout(timer);
  }, [playing, mode, cursor, replayFrames.length, speed, move, setPlaying]);
  useEffect(() => {
    const pause = () => { if (document.hidden) setPlaying(false); };
    document.addEventListener('visibilitychange', pause);
    return () => { document.removeEventListener('visibilitychange', pause); setPlaying(false); };
  }, [setPlaying]);
  return <div className={`playback-control ${mode}`}>
    <ToggleButtonGroup size="sm" selectionMode="single" disallowEmptySelection selectedKeys={new Set([mode])} onSelectionChange={keys => { if (keys.has('live')) live(); else if (keys.has('replay') && mode !== 'replay') replay(); }} aria-label="显示方式">
      <ToggleButton id="live"><span className="live-dot"/>实时观察</ToggleButton>
      <ToggleButton id="replay" isDisabled={!frames.length} aria-description={historyError || (historyLoading ? '正在读取本单的回放记录' : !frames.length ? '本单的处理记录尚未采集到' : '按已采集到的处理记录回看')}><PlayIcon/>教学回放</ToggleButton>
    </ToggleButtonGroup>
    {mode === 'replay' && <div className="playback-buttons">
      <Button variant="ghost" size="sm" isIconOnly aria-label="上一步" isDisabled={cursor === 0} onPress={() => { setPlaying(false); move(cursor - 1); }}><ChevronLeftIcon/></Button>
      <Button variant="outline" size="sm" aria-label={playing ? '暂停回放' : '播放回放'} isDisabled={replayFrames.length < 2} onPress={() => { if (!playing && cursor === replayFrames.length - 1) move(0); setPlaying(!playing); }}>{playing ? <PauseIcon/> : <PlayIcon/>}{playing ? '暂停' : cursor === replayFrames.length - 1 && replayFrames.length > 1 ? '重播' : '播放'}</Button>
      <Button variant="ghost" size="sm" isIconOnly aria-label="下一步" isDisabled={cursor >= replayFrames.length - 1} onPress={() => { setPlaying(false); move(cursor + 1); }}><ChevronRightIcon/></Button>
      <span className="frame-count" aria-live="polite">{cursor + 1} / {replayFrames.length}</span>
      <LabSelect label="回放速度" value={String(speed)} onChange={v => setSpeed(Number(v))} options={[{ value: '0.5', label: '0.5×' }, { value: '1', label: '1×' }, { value: '2', label: '2×' }]} className="speed-select"/>
      {frames.length > replayFrames.length && <Button variant="ghost" size="sm" onPress={replay}><ReloadIcon/>载入新步骤</Button>}
    </div>}
    {mode === 'live' && (historyLoading || historyError || !frames.length) && <span className="history-hint" role="status">{historyError || (historyLoading ? '正在读取本单回放…' : '等待采集本单处理记录')}</span>}
  </div>;
}
