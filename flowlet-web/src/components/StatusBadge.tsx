import { getStatusColor } from '@/lib/api';

export function StatusBadge({ status }: { status: string }) {
  const color = getStatusColor(status);
  return (
    <span style={{ backgroundColor: color }}
      className="inline-block px-2 py-0.5 rounded text-white text-xs font-semibold uppercase">
      {status}
    </span>
  );
}
