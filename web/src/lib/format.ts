/** Formatting shared by the pages. */

import type { FrameNode } from './api';

export function humanizeDuration(ms: number): string {
  if (ms < 1000) return `${Math.max(0, Math.round(ms))}ms`;
  let rest = ms;
  const d = Math.floor(rest / 86400000); rest %= 86400000;
  const h = Math.floor(rest / 3600000); rest %= 3600000;
  const m = Math.floor(rest / 60000); rest %= 60000;
  const s = Math.floor(rest / 1000);
  return [d && `${d}d`, h && `${h}h`, m && `${m}m`, (s || (!d && !h && !m)) && `${s}s`]
    .filter(Boolean).join(' ');
}

export function timeAgo(ts: string | null | undefined): string {
  if (!ts) return '—';
  const s = Math.floor((Date.now() - new Date(ts).getTime()) / 1000);
  if (s < 60) return `${Math.max(0, s)}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

export function elapsed(from: string | null, to: string | null): string {
  if (!from) return '—';
  const end = to ? new Date(to).getTime() : Date.now();
  return humanizeDuration(end - new Date(from).getTime());
}

export function shortEid(eid: string): string {
  return eid.slice(0, 8);
}

/** One colour per state, used by badges and by the flamegraph, so a bar and a
 *  badge for the same frame never disagree. */
export function statusColor(status: string): string {
  switch (status) {
    case 'completed': return '#16a34a';
    case 'failed': return '#dc2626';
    case 'cancelled': return '#64748b';
    case 'running': return '#2563eb';
    case 'suspended': return '#a855f7';
    case 'pending': return '#f59e0b';
    default: return '#9ca3af';
  }
}

export function statusClasses(status: string): string {
  switch (status) {
    case 'completed': return 'bg-green-100 text-green-800 ring-green-600/20';
    case 'failed': return 'bg-red-100 text-red-800 ring-red-600/20';
    case 'cancelled': return 'bg-slate-200 text-slate-700 ring-slate-600/20';
    case 'running': return 'bg-blue-100 text-blue-800 ring-blue-600/20';
    case 'suspended': return 'bg-purple-100 text-purple-800 ring-purple-600/20';
    case 'pending': return 'bg-amber-100 text-amber-800 ring-amber-600/20';
    default: return 'bg-gray-100 text-gray-700 ring-gray-500/20';
  }
}

/** Terminal statuses never change again (spec 01 section 2), so a view of one
 *  needs no polling. */
export function isTerminal(status: string): boolean {
  return status === 'completed' || status === 'failed' || status === 'cancelled';
}

export function json(value: unknown, indent = 2): string {
  if (value === undefined) return '';
  try {
    return JSON.stringify(value, null, indent) ?? String(value);
  } catch {
    return String(value);
  }
}

/** What a frame is waiting for, as a person reads it: `channel:{name}`,
 *  `timer:{id}` and `child:{eid}` are the engine's condition strings. */
export function describeCondition(on: string | null): string {
  if (!on) return '—';
  const [kind, ...rest] = on.split(':');
  const name = rest.join(':');
  switch (kind) {
    case 'channel': return `a message on ${name.split('.').slice(1).join('.') || name}`;
    case 'timer': return 'a timer';
    case 'child': return `child ${name.slice(0, 8)}`;
    case 'operator': return 'an operator';
    default: return on;
  }
}

export function walk(node: FrameNode): FrameNode[] {
  return [node, ...node.children.flatMap(walk)];
}
