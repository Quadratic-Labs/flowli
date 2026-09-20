import { statusClasses } from '@/lib/format';
import { cn } from '@/lib/utils';

export function StatusBadge({ status, className }: { status: string; className?: string }) {
  return (
    <span className={cn(
      'inline-flex items-center rounded px-2 py-0.5 text-xs font-medium uppercase tracking-wide ring-1 ring-inset',
      statusClasses(status), className,
    )}>
      {status}
    </span>
  );
}
