import { useEffect, useMemo, useState } from 'react';
import { useParams, useSearchParams } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { ChevronDown, ChevronRight } from 'lucide-react';
import { api, ApiError, getStatusColor, humanizeDuration, isCancellable, type ObligationEvent, type TraceSummaryDTO, type SpanRecordDTO } from '@/lib/api';
import { StatusBadge } from '@/components/StatusBadge';
import { ObligationFlamegraph } from '@/components/ObligationFlamegraph';
import { CancelObligationButton } from '@/components/CancelObligationButton';
import { ReviewPanel } from '@/components/ReviewPanel';

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
  tree: TraceSummaryDTO | null;
  logs: LogLine[];
}

/** Rebuild one attempt's summary tree from its span records — the client-side
 *  mirror of the server's analysis.summarise (which only returns the latest
 *  attempt), so every attempt can drive the flamegraph and children table. */
function buildAttemptTree(spans: SpanRecordDTO[]): TraceSummaryDTO | null {
  const nodes = new Map<string, TraceSummaryDTO>();
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
  let root: TraceSummaryDTO | null = null;
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

const TERMINAL_EVENTS = ['completed', 'failed', 'canceled'];

/** Statuses the kernel treats as closed (ReportedStatus.is_closed(), mirrored).
 *  Distinct from TERMINAL_EVENTS: a "reviewed" event can close a obligation
 *  (approved) or reopen it (rejected, budget left) — the resulting status,
 *  not the event name, says which. */
const CLOSED_STATUSES = ['completed', 'failed', 'canceled', 'warning', 'stopped'];

/** Map the latest lifecycle event to a displayable status for obligations whose
 *  span record does not exist yet (spans export only on completion). */
function eventToStatus(event: string | undefined): string {
  if (!event) return 'queued';
  if (event === 'submitted') return 'queued';
  if (event === 'claimed') return 'running';
  if (event === 'retry_scheduled' || event === 'requeued') return 'pending';
  if (event === 'cancel_requested') return 'canceling';
  if (event === 'awaiting_review') return 'gated';
  return event; // completed / failed / canceled / reviewed map to themselves
}

/** The account's authoritative status, from the last lifecycle event that
 *  recorded one (`to`) — falls back to the span-derived status when no such
 *  event exists yet.  The two can disagree: a gated obligation's callable already
 *  returned (its span says "completed"), but the account may have since been
 *  canceled or rejected instead of discharged, and the account is the one
 *  that actually decides whether the obligation's contract was met. */
function accountStatus(events: ObligationEvent[], fallback: string): string {
  for (let i = events.length - 1; i >= 0; i--) {
    const to = events[i].to;
    if (to) return to;
  }
  return fallback;
}

function eventBadgeClass(event: string): string {
  if (event === 'completed') return 'text-green-700 bg-green-100';
  if (event === 'failed') return 'text-red-700 bg-red-100';
  if (event === 'canceled' || event === 'cancel_requested') return 'text-slate-700 bg-slate-200';
  if (event === 'retry_scheduled' || event === 'requeued') return 'text-amber-700 bg-amber-100';
  if (event === 'claimed') return 'text-blue-700 bg-blue-100';
  return 'text-gray-700 bg-gray-100';
}

function EventTimeline({ events }: { events: ObligationEvent[] }) {
  return (
    <table className="w-full text-sm">
      <thead className="bg-gray-100 text-left">
        <tr>{['Time', 'Event', 'Actor', 'Attempt', 'Transition', 'Cause'].map(h =>
          <th key={h} className="py-2 px-3">{h}</th>)}</tr>
      </thead>
      <tbody>{events.map((e, i) => (
        <tr key={i} className="border-t">
          <td className="py-2 px-3 text-gray-500 font-mono text-xs whitespace-nowrap">
            {new Date(e.ts).toISOString().replace('T', ' ').slice(0, 23)}
          </td>
          <td className="py-2 px-3">
            <span className={`px-2 py-0.5 rounded text-xs font-semibold ${eventBadgeClass(e.event)}`}>
              {e.event}
            </span>
          </td>
          <td className="py-2 px-3 text-gray-500 font-mono text-xs">{e.actor}</td>
          <td className="py-2 px-3 text-gray-500">{e.attempt ?? '—'}</td>
          <td className="py-2 px-3 text-gray-500 text-xs">
            {e.from || e.to ? `${e.from ?? '·'} → ${e.to ?? '·'}` : '—'}
          </td>
          <td className="py-2 px-3 text-gray-500 text-xs">{e.cause ?? '—'}</td>
        </tr>
      ))}</tbody>
    </table>
  );
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

export default function ObligationDetail() {
  const { id: obligationId = '' } = useParams<{ id: string }>();
  const [searchParams] = useSearchParams();
  const parentId = searchParams.get('parentId');
  const flowHint = searchParams.get('flow');
  const queryClient = useQueryClient();

  // A 404 is not an error here: spans export only when a span completes and
  // the read cache only learns about a obligation after a worker claims it, so a
  // freshly submitted obligation legitimately has nothing to show yet.  Model that
  // as *data* (null) rather than a query error: React Query drops an
  // errored data-less query back to pending on every interval refetch,
  // which would flip isLoading and remount the whole page twice per poll.
  // With null the state is referentially stable and idle polls render nothing.
  const fetchObligationOrNull = async (id: string) => {
    try {
      return await api.getObligationById(id);
    } catch (e) {
      if (e instanceof ApiError && e.status === 404) return null;
      throw e;
    }
  };
  const { data: obligation, isLoading, error } = useQuery({
    queryKey: ['obligation', obligationId],
    queryFn: () => fetchObligationOrNull(obligationId),
    retry: false,
    refetchInterval: (q) => (q.state.data ? false : 2500),
  });
  const { data: parent } = useQuery({
    queryKey: ['obligation', parentId], queryFn: () => fetchObligationOrNull(parentId!),
    enabled: !!parentId,
  });
  // Lifecycle events are optional: older obligations (pre event log) have none and
  // deployments without storage don't expose the endpoint at all.  While the
  // obligation is in flight they are the only live signal, so keep polling until
  // the timeline reaches a terminal event.
  const { data: events = [] } = useQuery({
    queryKey: ['obligation-events', obligationId],
    queryFn: () => api.getObligationEvents(obligationId),
    retry: false,
    refetchInterval: (q) => {
      const evs = q.state.data;
      if (!evs || evs.length === 0) return 3000;
      const last = evs[evs.length - 1].event;
      return TERMINAL_EVENTS.includes(last) ? false : 3000;
    },
  });

  // Each lifecycle advance may have produced new spans or a final state —
  // refresh the obligation record whenever the timeline grows.
  const eventCount = events.length;
  useEffect(() => {
    if (eventCount > 0) queryClient.invalidateQueries({ queryKey: ['obligation', obligationId] });
  }, [eventCount, obligationId, queryClient]);

  const obligationNotFoundYet = obligation === null;

  const attempts = useMemo(() => groupAttempts(obligation?.logs ?? []), [obligation]);
  const [selectedAttempt, setSelectedAttempt] = useState<number | null>(null);
  // Latest attempt by default; an out-of-range selection (e.g. after
  // navigating to another obligation) falls back to the latest as well.
  const effectiveAttempt =
    attempts.find(a => a.attempt === selectedAttempt)?.attempt
    ?? attempts.at(-1)?.attempt
    ?? 1;
  const current = attempts.find(a => a.attempt === effectiveAttempt) ?? null;

  // The server summary (latest attempt) is the fallback when no span records
  // are available; otherwise the selected attempt's rebuilt tree drives the page.
  const view = current?.tree ?? obligation ?? null;
  const logs = current?.logs ?? [];
  const children = view?.children ?? [];

  const lastEvent = events.at(-1)?.event;

  // The account, not the span tree, decides whether the obligation's contract was
  // met — a gated obligation's callable already returned (span status "completed")
  // but may since have been canceled or rejected instead of discharged.
  const displayStatus = accountStatus(events, view?.status ?? '');
  const isGated = displayStatus === 'gated';
  const obligationActive = events.length > 0
    ? !CLOSED_STATUSES.includes(displayStatus.toLowerCase())
    : isCancellable(view?.status ?? '');

  return (
    <div className="p-6">
      {isLoading && <div className="flex justify-center p-10"><div className="w-10 h-10 border-4 border-blue-600 border-t-transparent rounded-full animate-spin" /></div>}
      {error != null && <div className="text-red-600 bg-red-50 p-4 rounded mb-4">{String(error)}</div>}

      {obligationNotFoundYet && (
        <div className="fade-in">
          <div className="bg-white rounded shadow p-6 mb-4">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-3">
                <h2 className="text-lg font-medium">Obligation Details</h2>
                {events.length > 0 && obligationActive && <CancelObligationButton obligationId={obligationId} small />}
              </div>
              <span className="flex items-center gap-2 text-sm text-gray-500">
                <span className="w-4 h-4 border-2 border-blue-600 border-t-transparent rounded-full animate-spin" />
                live — refreshing automatically
              </span>
            </div>
            <div className="text-sm space-y-1">
              {flowHint && <p><strong>Flow:</strong> {flowHint}</p>}
              <p><strong>Obligation ID:</strong> <span className="font-mono text-xs">{obligationId}</span></p>
              <p><strong>Status:</strong> <StatusBadge status={eventToStatus(lastEvent)} /></p>
            </div>
            <p className="text-gray-500 text-sm mt-4">
              {events.length === 0
                ? 'Waiting for a worker to pick this obligation up — details appear as soon as execution is recorded.'
                : 'Execution in progress — the full record (flamegraph, logs) appears when the attempt finishes.'}
            </p>
          </div>

          {eventToStatus(lastEvent) === 'gated' && <ReviewPanel obligationId={obligationId} />}
        </div>
      )}

      {obligation && view && (
        <div className="fade-in">
          <div className="bg-white rounded shadow p-6 mb-4">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-3">
                <h2 className="text-lg font-medium">Obligation Details</h2>
                {obligationActive && <CancelObligationButton obligationId={obligationId} small />}
              </div>
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
                <p><strong>Obligation ID:</strong> <span className="font-mono text-xs">{obligationId}</span></p>
                <p><strong>Root span:</strong> <span className="font-mono text-xs">{view.span_id}</span></p>
                <p><strong>Status:</strong> <StatusBadge status={displayStatus} />
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

          {isGated && <ReviewPanel obligationId={obligationId} />}

          <Section title="Execution Flamegraph" icon={<span>⏱</span>} defaultOpen>
            <ObligationFlamegraph obligationData={view} />
          </Section>
        </div>
      )}

      {/* Shared slot: rendered in both the waiting and the full-record view,
          so the branch swap neither remounts the timeline nor resets the
          section's open/closed state. */}
      {events.length > 0 && (
        <Section title="Lifecycle Events" icon={<span>🧭</span>} defaultOpen={!obligation}>
          <EventTimeline events={events} />
        </Section>
      )}

      {obligation && view && (
        <div className="fade-in">
          <Section title="Logs" icon={<span>📄</span>}>
            {logs.length === 0
              ? <div className="bg-blue-50 text-blue-700 p-4 rounded">No logs found for this obligation.</div>
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
            <Section title="Child Obligations" icon={<span>🌿</span>}>
              <table className="w-full text-sm">
                <thead className="bg-gray-100 text-left">
                  <tr>{['Name','Obligation ID','Status'].map(h => <th key={h} className="py-2 px-3">{h}</th>)}</tr>
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
        </div>
      )}
    </div>
  );
}
