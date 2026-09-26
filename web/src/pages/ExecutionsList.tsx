import { useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { api, type ExecutionStatus } from '@/lib/api';
import { StatusBadge } from '@/components/StatusBadge';
import { elapsed, shortEid, timeAgo } from '@/lib/format';

const STATUSES: ExecutionStatus[] = [
  'pending', 'running', 'suspended', 'completed', 'failed', 'cancelled',
];

export default function ExecutionsList() {
  const [params, setParams] = useSearchParams();
  const status = params.get('status') ?? '';
  const workflow = params.get('workflow') ?? '';
  // Keyset cursors, so paging cannot skip or repeat a row when the list moves.
  const [cursors, setCursors] = useState<string[]>([]);
  const cursor = cursors[cursors.length - 1];

  const { data, isLoading, error } = useQuery({
    queryKey: ['executions', { status, workflow, cursor }],
    queryFn: () => api.executions({
      status: status || undefined, workflow: workflow || undefined, cursor, limit: 25,
    }),
    refetchInterval: 5_000,
  });

  function filter(next: Record<string, string>) {
    setCursors([]);
    setParams(prev => {
      const out = new URLSearchParams(prev);
      for (const [k, v] of Object.entries(next)) {
        if (v) out.set(k, v); else out.delete(k);
      }
      return out;
    });
  }

  return (
    <div className="space-y-4 p-6">
      <h1 className="text-2xl font-semibold text-slate-900">Executions</h1>

      <div className="flex flex-wrap items-center gap-2">
        <button
          onClick={() => filter({ status: '' })}
          className={`rounded px-2.5 py-1 text-sm ${!status ? 'bg-slate-800 text-white' : 'bg-white ring-1 ring-slate-300'}`}
        >all</button>
        {STATUSES.map(s => (
          <button
            key={s}
            onClick={() => filter({ status: s })}
            className={`rounded px-2.5 py-1 text-sm ${status === s ? 'bg-slate-800 text-white' : 'bg-white ring-1 ring-slate-300'}`}
          >{s}</button>
        ))}
        <input
          defaultValue={workflow}
          onKeyDown={e => e.key === 'Enter' && filter({ workflow: e.currentTarget.value })}
          placeholder="workflow…"
          className="ml-auto rounded border border-slate-300 px-2 py-1 text-sm"
        />
      </div>

      {error && <p className="rounded bg-red-50 p-3 text-red-700">{String(error)}</p>}

      <div className="overflow-x-auto rounded border border-slate-200 bg-white">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-left text-xs uppercase text-slate-500">
            <tr>
              {['Status', 'Workflow', 'Eid', 'Queue', 'Started', 'Elapsed', 'Worker'].map(h => (
                <th key={h} className="px-3 py-2 font-medium">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data?.items.map(e => (
              <tr key={e.eid} className="border-t border-slate-100 hover:bg-slate-50">
                <td className="px-3 py-2"><StatusBadge status={e.status} /></td>
                <td className="px-3 py-2">
                  <Link to={`/executions/${e.eid}`} className="font-medium text-indigo-600">
                    {e.workflow}
                  </Link>
                  <span className="ml-1 text-slate-400">v{e.version}</span>
                </td>
                <td className="px-3 py-2 font-mono text-xs text-slate-500">{shortEid(e.eid)}</td>
                <td className="px-3 py-2 text-slate-600">{e.queue}</td>
                <td className="px-3 py-2 text-slate-500">{timeAgo(e.created_at)}</td>
                <td className="px-3 py-2 text-slate-500">{elapsed(e.created_at, e.archived_at ?? (
                  e.status === 'completed' || e.status === 'failed' || e.status === 'cancelled'
                    ? e.updated_at : null
                ))}</td>
                <td className="px-3 py-2 text-slate-500">{e.worker_id ?? '—'}</td>
              </tr>
            ))}
            {isLoading && (
              <tr><td colSpan={7} className="px-3 py-6 text-center text-slate-400">loading…</td></tr>
            )}
            {!isLoading && !data?.items.length && (
              <tr><td colSpan={7} className="px-3 py-6 text-center text-slate-400">no executions</td></tr>
            )}
          </tbody>
        </table>
      </div>

      <div className="flex items-center gap-2">
        <button
          onClick={() => setCursors(c => c.slice(0, -1))}
          disabled={!cursors.length}
          className="rounded border border-slate-300 px-3 py-1 text-sm disabled:opacity-40"
        >previous</button>
        <button
          onClick={() => data?.next_cursor && setCursors(c => [...c, data.next_cursor!])}
          disabled={!data?.next_cursor}
          className="rounded border border-slate-300 px-3 py-1 text-sm disabled:opacity-40"
        >next</button>
      </div>
    </div>
  );
}
