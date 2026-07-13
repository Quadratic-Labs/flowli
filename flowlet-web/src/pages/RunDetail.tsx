import { useMemo, useState } from 'react';
import { useParams, useSearchParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { ChevronDown, ChevronRight } from 'lucide-react';
import { api, getStatusColor, humanizeDuration, type RunSummaryDTO, type SpanRecordDTO } from '@/lib/api';
import { StatusBadge } from '@/components/StatusBadge';
import { RunFlamegraph } from '@/components/RunFlamegraph';

interface LogLine { ts: string | null; level: string; message: string }

/** Flatten span records (one per flow/task execution) into a chronological
 *  log timeline: a start line per span, its recorded events, and an end line
 *  carrying the span's outcome. */
function spansToLogLines(spans: SpanRecordDTO[]): LogLine[] {
  const lines: LogLine[] = [];
  for (const span of spans) {
    const name = `${span.span_type ?? 'span'} '${span.name ?? '?'}'`;
    lines.push({ ts: span.start_ts, level: 'INFO', message: `Starting ${name}` });
    for (const evt of span.events ?? []) {
      lines.push({
        ts: evt.ts,
        level: String(evt.attributes?.['log.level'] ?? 'INFO'),
        message: evt.message ?? '',
      });
    }
    lines.push(
      span.status === 'failed'
        ? { ts: span.end_ts, level: 'ERROR', message: `Failed ${name}${span.status_message ? `: ${span.status_message}` : ''}` }
        : { ts: span.end_ts, level: 'SUCCESS', message: `Completed ${name}` },
    );
  }
  return lines.sort((a, b) => (a.ts ?? '').localeCompare(b.ts ?? ''));
}

interface AttemptView {
  attempt: number;
  tree: RunSummaryDTO | null;
  logs: LogLine[];
}

/** Rebuild one attempt's summary tree from its span records — the client-side
 *  mirror of the server's analysis.summarise (which only returns the latest
 *  attempt), so every attempt can drive the flamegraph and children table. */
function buildAttemptTree(spans: SpanRecordDTO[]): RunSummaryDTO | null {
  const nodes = new Map<string, RunSummaryDTO>();
  for (const s of spans) {
    if (!s.span_id) continue;
    const start = s.start_ts ?? '';
    const end = s.end_ts ?? '';
    nodes.set(s.span_id, {
      span_id: s.span_id,
      span_name: s.name ?? '?',
      status: s.status ?? 'completed',
      start_ts: start,
      end_ts: end,
      duration: start && end
        ? humanizeDuration(new Date(end).getTime() - new Date(start).getTime())
        : null,
      children: [],
    });
  }
  let root: RunSummaryDTO | null = null;
  for (const s of spans) {
    if (!s.span_id) continue;
    const node = nodes.get(s.span_id)!;
    if (s.parent_span_id && nodes.has(s.parent_span_id)) {
      nodes.get(s.parent_span_id)!.children.push(node);
    } else {
      root = node;
    }
  }
  return root;
}

/** Group span records into per-attempt views, oldest attempt first. */
function groupAttempts(spans: SpanRecordDTO[]): AttemptView[] {
  const byAttempt = new Map<number, SpanRecordDTO[]>();
  for (const s of spans) {
    const a = s.attempt ?? 1;
    (byAttempt.get(a) ?? byAttempt.set(a, []).get(a)!).push(s);
  }
  return [...byAttempt.entries()]
    .sort(([a], [b]) => a - b)
    .map(([attempt, attemptSpans]) => ({
      attempt,
      tree: buildAttemptTree(attemptSpans),
      logs: spansToLogLines(attemptSpans),
    }));
}

function LogLevel({ level }: { level: string }) {
  const l = level.toLowerCase();
  const cls = l === 'info' ? 'text-sky-400 bg-sky-900/30'
    : l === 'error' || l === 'failed' ? 'text-red-400 bg-red-900/30'
    : l === 'warn' || l === 'warning' ? 'text-amber-400 bg-amber-900/30'
    : l === 'debug' ? 'text-purple-400 bg-purple-900/30'
    : 'text-green-400 bg-green-900/30';
  return <span className={`font-bold px-2 py-0.5 rounded text-xs inline-block min-w-[64px] text-center uppercase ${cls}`}>{level}</span>;
}

function Section({ title, icon, defaultOpen = false, children }: { title: string; icon: React.ReactNode; defaultOpen?: boolean; children: React.ReactNode }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="bg-white rounded shadow mb-4">
      <button onClick={() => setOpen(o => !o)}
        className="w-full flex items-center gap-2 p-4 text-left font-medium hover:bg-gray-50">
        {icon} {title} {open ? <ChevronDown size={16} className="ml-auto" /> : <ChevronRight size={16} className="ml-auto" />}
      </button>
      {open && <div className="px-4 pb-4">{children}</div>}
    </div>
  );
}

export default function RunDetail() {
  const { id: runId = '' } = useParams<{ id: string }>();
  const [searchParams] = useSearchParams();
  const parentId = searchParams.get('parentId');

  const { data: run, isLoading, error } = useQuery({
    queryKey: ['run', runId], queryFn: () => api.getRunById(runId),
  });
  const { data: parent } = useQuery({
    queryKey: ['run', parentId], queryFn: () => api.getRunById(parentId!),
    enabled: !!parentId,
  });

  const attempts = useMemo(() => groupAttempts(run?.logs ?? []), [run]);
  const [selectedAttempt, setSelectedAttempt] = useState<number | null>(null);
  // Latest attempt by default; an out-of-range selection (e.g. after
  // navigating to another run) falls back to the latest as well.
  const effectiveAttempt =
    attempts.find(a => a.attempt === selectedAttempt)?.attempt
    ?? attempts.at(-1)?.attempt
    ?? 1;
  const current = attempts.find(a => a.attempt === effectiveAttempt) ?? null;

  // The server summary (latest attempt) is the fallback when no span records
  // are available; otherwise the selected attempt's rebuilt tree drives the page.
  const view = current?.tree ?? run ?? null;
  const logs = current?.logs ?? [];
  const children = view?.children ?? [];

  return (
    <div className="p-6">
      {isLoading && <div className="flex justify-center p-10"><div className="w-10 h-10 border-4 border-blue-600 border-t-transparent rounded-full animate-spin" /></div>}
      {error && <div className="text-red-600 bg-red-50 p-4 rounded">{String(error)}</div>}

      {run && view && !isLoading && (
        <>
          <div className="bg-white rounded shadow p-6 mb-4">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-medium">Run Details</h2>
              {attempts.length > 1 && (
                <div className="flex items-center gap-2">
                  <span className="text-sm text-gray-500">Attempt:</span>
                  {attempts.map(a => (
                    <button
                      key={a.attempt}
                      onClick={() => setSelectedAttempt(a.attempt)}
                      title={`Attempt ${a.attempt} — ${a.tree?.status ?? 'unknown'}`}
                      className={`px-3 py-1 rounded-full text-sm border flex items-center gap-1.5 transition-colors ${
                        a.attempt === effectiveAttempt
                          ? 'bg-blue-600 text-white border-blue-600'
                          : 'bg-white text-gray-700 hover:bg-gray-50 border-gray-300'
                      }`}
                    >
                      #{a.attempt}
                      <span
                        className="w-2 h-2 rounded-full"
                        style={{ background: getStatusColor(a.tree?.status ?? '') }}
                      />
                    </button>
                  ))}
                </div>
              )}
            </div>
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <p><strong>Name:</strong> {view.span_name}</p>
                <p><strong>Run ID:</strong> <span className="font-mono text-xs">{runId}</span></p>
                <p><strong>Root span:</strong> <span className="font-mono text-xs">{view.span_id}</span></p>
                <p><strong>Status:</strong> <StatusBadge status={view.status} />
                  {attempts.length > 1 && (
                    <span className="text-gray-500 ml-2">attempt {effectiveAttempt} of {attempts.at(-1)?.attempt}</span>
                  )}
                </p>
              </div>
              <div>
                {parent && <p><strong>Parent:</strong> {parent.span_name} ({parent.span_id})</p>}
                {view.duration && <p><strong>Duration:</strong> {view.duration}</p>}
              </div>
            </div>
          </div>

          <Section title="Execution Flamegraph" icon={<span>⏱</span>} defaultOpen>
            <RunFlamegraph runData={view} />
          </Section>

          <Section title="Logs" icon={<span>📄</span>}>
            {logs.length === 0
              ? <div className="bg-blue-50 text-blue-700 p-4 rounded">No logs found for this run.</div>
              : <div className="bg-[#1e1e1e] text-[#d4d4d4] font-mono text-[13px] p-4 rounded max-h-[600px] overflow-auto">
                  {logs.map((log, i) => (
                    <div key={i} className="mb-3">
                      <div className="flex items-center gap-3 mb-1">
                        <span className="text-[#858585] min-w-[180px]">{log.ts ? new Date(log.ts).toISOString().replace('T',' ').slice(0,23) : ''}</span>
                        <LogLevel level={log.level ?? 'info'} />
                      </div>
                      <div className="whitespace-pre-wrap break-words leading-relaxed">{log.message}</div>
                    </div>
                  ))}
                </div>
            }
          </Section>

          {children.length > 0 && (
            <Section title="Child Runs" icon={<span>🌿</span>}>
              <table className="w-full text-sm">
                <thead className="bg-gray-100 text-left">
                  <tr>{['Name','Run ID','Status'].map(h => <th key={h} className="py-2 px-3">{h}</th>)}</tr>
                </thead>
                <tbody>{children.map((c, i) => (
                  <tr key={i} className="border-t">
                    <td className="py-2 px-3 font-medium">{c.span_name}</td>
                    <td className="py-2 px-3 text-gray-500 font-mono text-xs">{c.span_id}</td>
                    <td className="py-2 px-3"><StatusBadge status={c.status} /></td>
                  </tr>
                ))}</tbody>
              </table>
            </Section>
          )}
        </>
      )}
    </div>
  );
}
