import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { Activity, AlertTriangle, Clock, Inbox, Layers, Play, Workflow } from 'lucide-react';
import { api, type ExecutionStatus, type WorkflowStats } from '@/lib/api';
import { StatusBadge } from '@/components/StatusBadge';
import { humanizeDuration, shortEid, statusColor, timeAgo } from '@/lib/format';

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
  const workflowStats = useQuery({
    queryKey: ['workflow-stats'], queryFn: api.workflowStats, refetchInterval: 10_000,
  });
  const catalog = useQuery({ queryKey: ['workflows'], queryFn: api.workflows, refetchInterval: 30_000 });

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

      <Card title="Workflows" icon={<Workflow size={16} />} to="/workflows">
        <WorkflowTable
          stats={workflowStats.data?.items ?? []}
          catalog={catalog.data?.items ?? []}
          loading={workflowStats.isLoading || catalog.isLoading}
        />
      </Card>
    </div>
  );
}

/** How many past runs the dots show, newest on the right. */
const RECENT = 5;

interface WorkflowLine {
  name: string;
  stats: WorkflowStats | undefined;
  /** The version Start opens: the highest one this process registers. */
  version: string | undefined;
}

/** The catalog and the projection, one line per workflow name. A name the
 *  catalog lacks ran on another deployment: it has stats and no Start. A name
 *  that never ran has a Start and grey dots. */
function lines(stats: WorkflowStats[], catalog: { name: string; version: string }[]): WorkflowLine[] {
  const byName = new Map<string, WorkflowLine>();
  for (const wf of catalog) {
    const line = byName.get(wf.name) ?? { name: wf.name, stats: undefined, version: undefined };
    if (!line.version || wf.version.localeCompare(line.version, undefined, { numeric: true }) > 0) {
      line.version = wf.version;
    }
    byName.set(wf.name, line);
  }
  for (const s of stats) {
    const line = byName.get(s.workflow) ?? { name: s.workflow, stats: undefined, version: undefined };
    line.stats = s;
    byName.set(s.workflow, line);
  }
  return [...byName.values()].sort((a, b) => a.name.localeCompare(b.name));
}

function WorkflowTable({ stats, catalog, loading }: {
  stats: WorkflowStats[]; catalog: { name: string; version: string }[]; loading: boolean;
}) {
  const rows = lines(stats, catalog);
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="text-left text-xs uppercase text-slate-500">
          <tr>
            <th className="py-1">Workflow</th>
            <th>Last {RECENT}</th>
            <th className="text-right">Runs</th>
            <th className="text-right">Active</th>
            <th className="text-right" title="Completed, among the last 20 finished runs">Success</th>
            <th className="text-right" title="Of the completed runs among the last 20 finished, from creation to end">
              Avg duration
            </th>
            <th className="pl-4">Last run</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {rows.map(({ name, stats: s, version }) => (
            <tr key={name} className="border-t border-slate-100">
              <td className="py-1.5">
                <Link
                  to={`/executions?workflow=${encodeURIComponent(name)}`}
                  className="font-medium text-indigo-600 hover:underline"
                >{name}</Link>
              </td>
              <td><RecentDots recent={s?.recent ?? []} /></td>
              <td className="text-right tabular-nums">{s?.runs ?? 0}</td>
              <td className="text-right tabular-nums text-slate-600">{s?.active || '—'}</td>
              <td className="text-right tabular-nums text-slate-600">
                {s?.finished ? `${Math.round((100 * s.completed) / s.finished)}%` : '—'}
              </td>
              <td className="text-right tabular-nums text-slate-600">
                {s?.avg_duration_ms != null ? humanizeDuration(s.avg_duration_ms) : '—'}
              </td>
              <td className="pl-4 text-slate-500" title={s?.last_run_at}>
                {s ? timeAgo(s.last_run_at) : 'never'}
              </td>
              <td className="py-1.5 text-right">
                {version ? (
                  <Link
                    to={`/workflows/${encodeURIComponent(name)}/${encodeURIComponent(version)}/start`}
                    className="inline-flex items-center gap-1 rounded bg-indigo-600 px-2.5 py-1 text-xs text-white hover:bg-indigo-700"
                    title={`Start ${name} v${version}`}
                  ><Play size={12} /> Start</Link>
                ) : (
                  <span className="text-xs text-slate-400" title="This process does not register it">
                    not here
                  </span>
                )}
              </td>
            </tr>
          ))}
          {!loading && !rows.length && (
            <tr><td colSpan={8} className="py-3 text-slate-400">no workflow</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

/** The last runs, oldest on the left. A slot with no run yet stays grey. */
function RecentDots({ recent }: { recent: ExecutionStatus[] }) {
  const slots: (ExecutionStatus | null)[] = [
    ...Array<null>(Math.max(0, RECENT - recent.length)).fill(null),
    ...recent.slice(-RECENT),
  ];
  return (
    <span className="inline-flex items-center gap-1" aria-label={recent.join(', ') || 'no run yet'}>
      {slots.map((status, i) => (
        <span
          key={i}
          title={status ?? 'no run yet'}
          className={`inline-block h-3 w-3 rounded-full ${status === 'running' ? 'animate-pulse' : ''}`}
          style={{ backgroundColor: status ? statusColor(status) : '#e2e8f0' }}
        />
      ))}
    </span>
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
