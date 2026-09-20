import { useMemo, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { ChevronDown, ChevronRight } from 'lucide-react';
import {
  api, ApiError,
  type EvidenceItem, type FrameNode, type JournalEntry,
} from '@/lib/api';
import { StatusBadge } from '@/components/StatusBadge';
import { FrameFlamegraph } from '@/components/FrameFlamegraph';
import { EvidencePanel } from '@/components/EvidencePanel';
import { ExecutionActions } from '@/components/ExecutionActions';
import { ReviewPanel } from '@/components/ReviewPanel';
import {
  describeCondition, elapsed, isTerminal, json, shortEid, statusColor, timeAgo, walk,
} from '@/lib/format';

type Tab = 'frames' | 'journal' | 'children';

export default function ExecutionDetail() {
  const { eid = '' } = useParams<{ eid: string }>();
  const [tab, setTab] = useState<Tab>('frames');
  const [selected, setSelected] = useState<string | null>(null);

  const execution = useQuery({
    queryKey: ['execution', eid],
    queryFn: () => api.execution(eid),
    // A terminal execution never changes again (spec 01 section 2): stop polling.
    refetchInterval: q => (q.state.data && isTerminal(q.state.data.status) ? false : 2_000),
  });
  const terminal = execution.data ? isTerminal(execution.data.status) : false;

  const frames = useQuery({
    queryKey: ['frames', eid],
    queryFn: () => api.frames(eid),
    refetchInterval: terminal ? false : 2_000,
  });
  const journal = useQuery({
    queryKey: ['journal', eid],
    queryFn: () => api.journal(eid),
    refetchInterval: terminal ? false : 5_000,
  });
  const children = useQuery({
    queryKey: ['children', eid],
    queryFn: () => api.children(eid),
    enabled: tab === 'children',
  });
  const evidence = useQuery({
    queryKey: ['evidence', eid],
    queryFn: () => api.evidence(eid),
    refetchInterval: terminal ? false : 15_000,
  });
  const reviews = useQuery({
    queryKey: ['reviews'],
    queryFn: () => api.reviews(),
    refetchInterval: terminal ? false : 10_000,
  });

  const pendingReviews = (reviews.data?.items ?? []).filter(r => r.eid === eid);
  const nodes = useMemo(
    () => (frames.data?.root ? walk(frames.data.root) : []),
    [frames.data],
  );
  const current = nodes.find(n => n.fid === selected) ?? null;
  const evidenceFor = (fid: string): EvidenceItem[] =>
    (evidence.data?.items ?? []).filter(i => i.fid === fid);

  if (execution.error instanceof ApiError && execution.error.status === 404) {
    return <p className="p-6 text-slate-600">No execution {eid}. It may have been archived away.</p>;
  }

  const record = execution.data;

  return (
    <div className="space-y-4 p-6">
      <header className="flex flex-wrap items-center gap-3">
        <h1 className="text-2xl font-semibold text-slate-900">
          {record?.workflow ?? '…'}
          {record && <span className="ml-2 text-lg font-normal text-slate-400">v{record.version}</span>}
        </h1>
        {record && <StatusBadge status={record.status} />}
        <span className="font-mono text-sm text-slate-500">{eid}</span>
      </header>

      {record && (
        <div className="grid gap-4 lg:grid-cols-3">
          <dl className="col-span-2 grid grid-cols-2 gap-x-6 gap-y-2 rounded border border-slate-200 bg-white p-4 text-sm md:grid-cols-3">
            <Field label="Queue">{record.queue}</Field>
            <Field label="Started">{timeAgo(record.created_at)}</Field>
            <Field label="Elapsed">
              {elapsed(record.created_at, isTerminal(record.status) ? record.updated_at : null)}
            </Field>
            <Field label="By">{record.created_by.id ?? '—'}</Field>
            <Field label="Worker">{record.worker_id ?? '—'}{record.epoch ? ` · epoch ${record.epoch}` : ''}</Field>
            <Field label="Dispatch key">{record.dispatch_key ?? '—'}</Field>
            {record.parent_eid && (
              <Field label="Parent">
                <Link to={`/executions/${record.parent_eid}`} className="text-indigo-600">
                  {shortEid(record.parent_eid)}
                </Link>
              </Field>
            )}
            {record.suspended_on && (
              <Field label="Waiting for" wide>
                {record.suspended_on.map(describeCondition).join(', ')}
              </Field>
            )}
            {record.error && (
              <Field label="Error" wide>
                <span className="text-red-700">{record.error.type}: {record.error.message}</span>
              </Field>
            )}
            {record.result !== null && record.result !== undefined && (
              <Field label="Result" wide>
                <pre className="mt-1 max-h-32 overflow-auto rounded bg-slate-50 p-2 text-xs">{json(record.result)}</pre>
              </Field>
            )}
          </dl>
          <ExecutionActions execution={record} />
        </div>
      )}

      {pendingReviews.map(review => (
        <ReviewPanel key={review.rid} review={review} onDecided={() => execution.refetch()} />
      ))}

      <nav className="flex gap-1 border-b border-slate-200">
        {(['frames', 'journal', 'children'] as Tab[]).map(name => (
          <button
            key={name}
            onClick={() => setTab(name)}
            className={`px-3 py-2 text-sm capitalize ${
              tab === name
                ? 'border-b-2 border-indigo-600 font-medium text-indigo-700'
                : 'text-slate-500 hover:text-slate-800'
            }`}
          >{name}</button>
        ))}
      </nav>

      {tab === 'frames' && (
        <div className="space-y-4">
          {frames.data?.root
            ? <div className="rounded border border-slate-200 bg-white p-3">
                <FrameFlamegraph root={frames.data.root} onSelect={setSelected} />
              </div>
            : <p className="text-slate-400">No frame has run yet.</p>}

          <div className="grid gap-4 lg:grid-cols-2">
            <FrameTree nodes={frames.data?.root ? [frames.data.root] : []} selected={selected} onSelect={setSelected} />
            <div className="rounded border border-slate-200 bg-white p-4">
              {current ? (
                <div className="space-y-3">
                  <div className="flex items-center gap-2">
                    <StatusBadge status={current.status} />
                    <span className="font-medium">{current.name}</span>
                    <span className="text-xs text-slate-400">{current.kind}</span>
                  </div>
                  <p className="font-mono text-xs text-slate-500">{current.fid}</p>
                  {current.attempts > 1 && (
                    <p className="text-sm text-amber-700">{current.attempts} attempts</p>
                  )}
                  {current.suspended_on && (
                    <p className="text-sm text-purple-700">
                      waiting for {describeCondition(current.suspended_on)}
                      {current.deadline && ` until ${current.deadline.slice(0, 19)}`}
                    </p>
                  )}
                  {current.error && (
                    <pre className="overflow-auto rounded bg-red-50 p-2 text-xs text-red-800">
                      {current.error.type}: {current.error.message}
                    </pre>
                  )}
                  {current.value !== null && current.value !== undefined && (
                    <pre className="max-h-40 overflow-auto rounded bg-slate-50 p-2 text-xs">{json(current.value)}</pre>
                  )}
                  <EvidencePanel eid={eid} fid={current.fid} items={evidenceFor(current.fid)} />
                </div>
              ) : (
                <p className="text-sm text-slate-400">Pick a frame to see its memo and its evidence.</p>
              )}
            </div>
          </div>
        </div>
      )}

      {tab === 'journal' && <Journal entries={journal.data?.items ?? []} />}

      {tab === 'children' && (
        <ul className="divide-y divide-slate-100 rounded border border-slate-200 bg-white text-sm">
          {children.data?.items.map(child => (
            <li key={child.eid} className="flex items-center gap-3 px-3 py-2">
              <StatusBadge status={child.status} />
              <Link to={`/executions/${child.eid}`} className="font-medium text-indigo-600">
                {child.workflow}
              </Link>
              <span className="font-mono text-xs text-slate-400">{shortEid(child.eid)}</span>
              <span className="ml-auto text-slate-400">{timeAgo(child.created_at)}</span>
            </li>
          ))}
          {!children.data?.items.length && <li className="px-3 py-6 text-slate-400">no children</li>}
        </ul>
      )}
    </div>
  );
}

function Field({ label, children, wide }: {
  label: string; children: React.ReactNode; wide?: boolean;
}) {
  return (
    <div className={wide ? 'col-span-2 md:col-span-3' : ''}>
      <dt className="text-xs uppercase tracking-wide text-slate-400">{label}</dt>
      <dd className="text-slate-800">{children}</dd>
    </div>
  );
}

function FrameTree({ nodes, selected, onSelect }: {
  nodes: FrameNode[]; selected: string | null; onSelect: (fid: string) => void;
}) {
  return (
    <div className="rounded border border-slate-200 bg-white p-2 text-sm">
      {nodes.map(node => <TreeRow key={node.fid} node={node} depth={0} selected={selected} onSelect={onSelect} />)}
      {!nodes.length && <p className="p-2 text-slate-400">nothing yet</p>}
    </div>
  );
}

function TreeRow({ node, depth, selected, onSelect }: {
  node: FrameNode; depth: number; selected: string | null; onSelect: (fid: string) => void;
}) {
  const [open, setOpen] = useState(true);
  const hasChildren = node.children.length > 0;
  return (
    <>
      <div
        onClick={() => onSelect(node.fid)}
        className={`flex cursor-pointer items-center gap-1.5 rounded px-1.5 py-1 hover:bg-slate-50 ${
          selected === node.fid ? 'bg-indigo-50' : ''
        }`}
        style={{ paddingLeft: depth * 14 + 6 }}
      >
        {hasChildren
          ? <button onClick={e => { e.stopPropagation(); setOpen(o => !o); }} className="text-slate-400">
              {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
            </button>
          : <span className="w-3.5" />}
        <span className="h-2 w-2 shrink-0 rounded-full" style={{ background: statusColor(node.status) }} />
        <span className="font-medium text-slate-800">{node.name}</span>
        <span className="text-xs text-slate-400">{node.kind}</span>
        {node.attempts > 1 && <span className="text-xs text-amber-600">×{node.attempts}</span>}
        {node.evidence && <span className="text-xs text-slate-400" title="has evidence">▤</span>}
        <span className="ml-auto text-xs text-slate-400">
          {elapsed(node.started_at, node.ended_at)}
        </span>
      </div>
      {open && node.children.map(child => (
        <TreeRow key={child.fid} node={child} depth={depth + 1} selected={selected} onSelect={onSelect} />
      ))}
    </>
  );
}

const ENTRY_CLASS: Record<string, string> = {
  'execution.started': 'bg-blue-100 text-blue-800',
  'execution.completed': 'bg-green-100 text-green-800',
  'execution.failed': 'bg-red-100 text-red-800',
  'execution.cancelled': 'bg-slate-200 text-slate-700',
  'execution.suspended': 'bg-purple-100 text-purple-800',
  'execution.resumed': 'bg-blue-100 text-blue-800',
  'execution.migrated': 'bg-amber-100 text-amber-800',
  'frame.failed': 'bg-red-50 text-red-700',
  'frame.completed': 'bg-green-50 text-green-700',
};

function Journal({ entries }: { entries: JournalEntry[] }) {
  const [open, setOpen] = useState<number | null>(null);
  return (
    <div className="overflow-x-auto rounded border border-slate-200 bg-white">
      <table className="w-full text-sm">
        <thead className="bg-slate-50 text-left text-xs uppercase text-slate-500">
          <tr>{['Seq', 'Time', 'Type', 'Frame', 'Attempt', 'Actor'].map(h =>
            <th key={h} className="px-3 py-2 font-medium">{h}</th>)}</tr>
        </thead>
        <tbody>
          {entries.map(entry => (
            <>
              <tr
                key={entry.seq}
                onClick={() => setOpen(o => (o === entry.seq ? null : entry.seq))}
                className="cursor-pointer border-t border-slate-100 hover:bg-slate-50"
              >
                <td className="px-3 py-1.5 font-mono text-xs text-slate-400">{entry.seq}</td>
                <td className="px-3 py-1.5 text-slate-500">{entry.at.slice(11, 23)}</td>
                <td className="px-3 py-1.5">
                  <span className={`rounded px-1.5 py-0.5 text-xs ${ENTRY_CLASS[entry.type] ?? 'bg-slate-100 text-slate-700'}`}>
                    {entry.type}
                  </span>
                </td>
                <td className="px-3 py-1.5 font-mono text-xs text-slate-600">{entry.fid}</td>
                <td className="px-3 py-1.5 text-slate-500">{entry.attempt}</td>
                <td className="px-3 py-1.5 text-slate-500">
                  {entry.actor.kind}:{entry.actor.id}
                  {entry.actor.on_behalf_of && ` for ${entry.actor.on_behalf_of}`}
                </td>
              </tr>
              {open === entry.seq && (
                <tr key={`${entry.seq}-detail`} className="bg-slate-50">
                  <td colSpan={6} className="px-3 py-2">
                    <pre className="max-h-64 overflow-auto text-xs text-slate-700">{json(entry.payload)}</pre>
                    <p className="mt-1 text-xs text-slate-400">
                      {entry.code.workflow} v{entry.code.version} · {entry.code.frame_kind} {entry.code.frame_name}
                      {entry.site.worker_id && ` · ${entry.site.worker_id}@${entry.site.host}`}
                      {entry.site.epoch !== null && ` · epoch ${entry.site.epoch}`}
                      {entry.code.code_ref && ` · ${entry.code.code_ref}`}
                    </p>
                  </td>
                </tr>
              )}
            </>
          ))}
          {!entries.length && (
            <tr><td colSpan={6} className="px-3 py-6 text-center text-slate-400">the journal is empty</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
