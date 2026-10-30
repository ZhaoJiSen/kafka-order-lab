import { ListBox, Select } from '@heroui/react';

export function LabSelect({ label, value, options, onChange, className = '' }: { label: string; value: string; options: { value: string; label: string }[]; onChange: (value: string) => void; className?: string }) {
  return <Select aria-label={label} selectedKey={value} onSelectionChange={key => { if (key !== null) onChange(String(key)); }} className={className}>
    <Select.Trigger><Select.Value/><Select.Indicator/></Select.Trigger>
    <Select.Popover><ListBox>{options.map(option => <ListBox.Item key={option.value} id={option.value} textValue={option.label}>{option.label}<ListBox.ItemIndicator/></ListBox.Item>)}</ListBox></Select.Popover>
  </Select>;
}
