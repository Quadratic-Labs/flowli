import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { Activity, AlertTriangle, Clock, Inbox, Layers } from 'lucide-react';
import { api } from '@/lib/api';
import { StatusBadge } from '@/components/StatusBadge';
import { shortEid, timeAgo } from '@/lib/format';

export default function Dashboard() {
  const stats = useQuery({ queryKey: ['stats'], queryFn: api.stats, refetchInterval: 10_000 });
  const queues = useQuery({ queryKey: ['queues'], queryFn: api.queues, refetchInterval: 10_000 });
  const health = useQuery({ queryKey: ['health'], queryFn: api.health, refetchInterval: 30_000 });
  const failed = useQuery({
    queryKey: ['executions', { status: 'failed' }],
    queryFn: () => api.executions({ status: 'failed', limit: 8 }),
    refetchInterval: 15_000,
  });
  const reviews = useQuery({ queryKey: ['reviews'], queryFn: () => api.reviews(), refetchInterval: 15_000 });

  const counts = stats.data?.by_status ?? {};
  const total = Object.values(counts).reduce((a, b) => a + b, 0);

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold text-slate-900">Dashboard</h1>
        {health.data && (
          <span className={`text-sm ${health.data.status === 'ok' ? 'text-green-700' : 'text-amber-700'}`}>
            projection at {health.data.projection_seq ?? '—'} · {health.data.status}
          </span>
        )}
      </div>

      <div className="grid grid-cols-2 gap-4 md:grid-cols-4 lg:grid-cols-6">
        <Tile label="Executions" value={total} icon={<Layers size={16} />} />
        {(['running', 'suspended', 'completed', 'failed', 'cancelled'] as const).map(status => (
          <Tile
            key={status}
            label={status}
            value={counts[status] ?? 0}
            to={`/executions?status=${status}`}
            icon={status === 'failed' ? <AlertTriangle size={16} /> : <Activity size={16} />}
          />
        ))}
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card title="Queues" icon={<Inbox size={16} />} to="/queues">
          <table className="w-full text-sm">
            <thead className="text-left text-xs uppercase text-slate-500">
              <tr><th className="py-1">Queue</th><th>Total</th><th>Visible</th><th>Claimed</th></tr>
            </thead>
            <tbody>
              {queues.data?.items.map(q => (
                <tr key={q.queue} className="border-t border-slate-100">
                  <td className="py-1.5 font-medium">{q.queue}</td>
                  <td>{q.total}</td>
                  <td>{q.visible}</td>
                  {/* An upper bound: a dead holder's lease lingers until a steal. */}
                  <td className="text-slate-500">≤ {q.claimed}</td>
                </tr>
              ))}
              {!queues.data?.items.length && (
                <tr><td colSpan={4} className="py-3 text-slate-400">nothing queued</td></tr>
              )}
            </tbody>
          </table>
        </Card>

        <Card title="Reviews waiting" icon={<Clock size={16} />} to="/reviews">
          <ul className="divide-y divide-slate-100 text-sm">
            {reviews.data?.items.slice(0, 6).map(r => (
              <li key={r.rid} className="flex items-center justify-between py-2">
                <span>
                  <span className="font-medium">{r.queue}</span>
                  <span className="ml-2 text-slate-400">{timeAgo(r.requested_at)}</span>
                </span>
                {r.eid && (
                  <Link to={`/executions/${r.eid}`} className="font-mono text-xs text-indigo-600">
                    {shortEid(r.eid)}
                  </Link>
                )}
              </li>
            ))}
            {!reviews.data?.items.length && <li className="py-3 text-slate-400">nothing waiting</li>}
          </ul>
        </Card>
      </div>

      <Card title="Recent failures" icon={<AlertTriangle size={16} />} to="/executions?status=failed">
        <ul className="divide-y divide-slate-100 text-sm">
          {failed.data?.items.map(e => (
            <li key={e.eid} className="flex items-center gap-3 py-2">
              <StatusBadge status={e.status} />
              <Link to={`/executions/${e.eid}`} className="font-medium text-indigo-600">
                {e.workflow}<span className="text-slate-400"> v{e.version}</span>
              </Link>
              <span className="truncate text-slate-500">{e.error?.message}</span>
              <span className="ml-auto text-slate-400">{timeAgo(e.updated_at)}</span>
            </li>
          ))}
          {!failed.data?.items.length && <li className="py-3 text-slate-400">none</li>}
        </ul>
      </Card>
    </div>
  );
}

function Tile({ label, value, icon, to }: {
  label: string; value: number; icon: React.ReactNode; to?: string;
}) {
  const body = (
    <div className="rounded border border-slate-200 bg-white p-4">
      <div className="flex items-center gap-2 text-xs uppercase tracking-wide text-slate-500">
        {icon}{label}
      </div>
      <div className="mt-1 text-2xl font-semibold text-slate-900">{value}</div>
    </div>
  );
  return to ? <Link to={to} className="block hover:opacity-80">{body}</Link> : body;
}

function Card({ title, icon, to, children }: {
  title: string; icon: React.ReactNode; to?: string; children: React.ReactNode;
}) {
  return (
    <section className="rounded border border-slate-200 bg-white p-4">
      <header className="mb-2 flex items-center gap-2">
        <span className="text-slate-400">{icon}</span>
        <h2 className="font-medium text-slate-800">{title}</h2>
        <div className="grow" />
        {to && <Link to={to} className="text-sm text-indigo-600 hover:underline">all</Link>}
      </header>
      {children}
    </section>
  );
}
