import { useEffect, useRef, useState, type ReactNode } from 'react';
import { Button } from '@heroui/react';
import { CheckIcon, CopyIcon } from '@radix-ui/react-icons';

export function CopyButton({ value, label, className = '', children }: { value: string; label: string; className?: string; children?: ReactNode }) {
  const [result, setResult] = useState<{ value: string; ok: boolean } | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => () => clearTimeout(timer.current), []);
  const current = result?.value === value ? result : null;
  const copy = async () => {
    clearTimeout(timer.current);
    try { await navigator.clipboard.writeText(value); setResult({ value, ok: true }); }
    catch { setResult({ value, ok: false }); }
    timer.current = setTimeout(() => setResult(null), 2400);
  };
  return <Button type="button" variant="ghost" size="sm" isIconOnly={!children} className={className} aria-label={current?.ok ? '已复制' : label} onPress={() => void copy()}>
    {children}{current?.ok ? <CheckIcon/> : <CopyIcon/>}
    {current && <span className={`copy-feedback ${current.ok ? '' : 'error'}`} role="status">{current.ok ? '已复制' : '复制失败，请手动选择文字'}</span>}
  </Button>;
}
