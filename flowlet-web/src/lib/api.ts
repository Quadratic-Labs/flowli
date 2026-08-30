export interface TraceSummaryDTO {
  span_id: string; span_name: string; status: string;
  start_ts: string; end_ts: string; duration: string | null;
  children: TraceSummaryDTO[];
}
export interface SpanEventDTO {
  ts: string | null; message: string | null;
  attributes: Record<string, unknown>;
}
export interface SpanRecordDTO {
  obligation_id: string | null; span_id: string | null; parent_span_id: string | null;
  name: string | null; flow_name: string | null; attempt: number;
  span_type: string | null; status: string | null; status_message: string | null;
  start_ts: string | null; end_ts: string | null;
  events: SpanEventDTO[]; attributes: Record<string, unknown>;
}
export interface TraceDTO extends TraceSummaryDTO { logs: SpanRecordDTO[]; }
export interface FlowSchema {
  name: string; docstring?: string | null; has_schema: boolean;
  parameters?: Array<{ name: string; type: string; required: boolean; default?: unknown }>;
}
export interface ObligationSummaryDTO {
  obligation_id: string; flow_name: string; status: string; worker_id: string;
  started_at: string; ended_at: string | null;
  attempt: number; max_retries: number;
}
export interface FlowSubmissionResponse {
  job_id: string; obligation_id: string; status: string;
  submitted_at: string; deduplicated: boolean;
}
export interface CancelRunResponse {
  obligation_id: string; status: string; cancel_requested: boolean;
}
export interface ReviewResponse {
  obligation_id: string; status: string; decision: string;
}
export interface RunEvent {
  ts: string; obligation_id: string; flow_name: string; event: string; actor: string;
  attempt?: number; from?: string; to?: string; cause?: string;
  details?: Record<string, unknown>;
}
export interface DashboardMetrics {
  healthStatus: 'green' | 'orange' | 'red';
  totalFlows: number; successfulFlows: number; failedFlows: number; runningFlows: number;
  successRate: number; failureRate: number;
  failedRuns: ObligationSummaryDTO[]; longRunningFlows: ObligationSummaryDTO[]; gatedRuns: ObligationSummaryDTO[];
  totalComputeTime: string; averageExecutionTime: string; lastUpdated: Date;
}

const API = '/api';

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

/** ETag cache for GET endpoints: request with If-None-Match and, on a 304,
 *  hand back the exact same object as last time.  Referential stability is
 *  the point — React Query (and any memo/effect keyed on the data) then sees
 *  an unchanged value and nothing re-renders or redraws. */
const etagCache = new Map<string, { etag: string; data: unknown }>();

/** Failed polls get the same treatment: an identical failure (same status
 *  and body, e.g. the 404 while a run is still queued) re-throws the same
 *  Error instance, so `error` stays referentially stable across polls. */
const errorCache = new Map<string, { sig: string; error: ApiError }>();

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const method = (init?.method ?? 'GET').toUpperCase();
  const cached = method === 'GET' ? etagCache.get(path) : undefined;

  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (cached) headers['If-None-Match'] = cached.etag;

  const res = await fetch(`${API}${path}`, { ...init, headers: { ...headers, ...init?.headers } });

  if (res.status === 304 && cached) return cached.data as T;

  if (!res.ok) {
    const text = await res.text();
    const sig = `${res.status}:${text}`;
    const prev = errorCache.get(path);
    if (prev && prev.sig === sig) throw prev.error;
    const error = new ApiError(res.status, `API error ${res.status}: ${text}`);
    errorCache.set(path, { sig, error });
    throw error;
  }

  errorCache.delete(path);
  const data = (await res.json()) as T;
  const etag = res.headers.get('ETag');
  if (method === 'GET' && etag) etagCache.set(path, { etag, data });
  return data;
}

export const api = {
  getFlows: () => apiFetch<FlowSchema[]>('/flows'),
  submitFlow: (name: string, kwargs: Record<string, unknown> = {}, dispatchKey?: string) =>
    apiFetch<FlowSubmissionResponse>(`/submit/${name}`, {
      method: 'POST',
      body: JSON.stringify({ kwargs, ...(dispatchKey ? { dispatch_key: dispatchKey } : {}) }),
    }),
  queryRuns: (last_n = 50, names?: string[]) =>
    apiFetch<ObligationSummaryDTO[]>('/runs/query', { method: 'POST', body: JSON.stringify({ last_n, ...(names ? { names } : {}) }) }),
  getRunById: (obligationId: string, with_logs = true) =>
    apiFetch<TraceDTO>(`/runs/${obligationId}?with_logs=${with_logs}`),
  getRunEvents: (obligationId: string) => apiFetch<RunEvent[]>(`/runs/${obligationId}/events`),
  cancelRun: (obligationId: string) =>
    apiFetch<CancelRunResponse>(`/runs/${obligationId}/cancel`, { method: 'POST' }),
  reviewRun: (obligationId: string, decision: 'approved' | 'rejected', actor: string, reason?: string) =>
    apiFetch<ReviewResponse>(`/runs/${obligationId}/review`, {
      method: 'POST',
      body: JSON.stringify({ decision, actor, ...(reason ? { reason } : {}) }),
    }),
};

export function humanizeDuration(ms: number): string {
  if (ms === 0) return '0s';
  const d = Math.floor(ms / 86400000); ms %= 86400000;
  const h = Math.floor(ms / 3600000); ms %= 3600000;
  const m = Math.floor(ms / 60000); ms %= 60000;
  const s = Math.floor(ms / 1000);
  return [d && `${d}d`, h && `${h}h`, m && `${m}m`, (s || (!d && !h && !m)) && `${s}s`].filter(Boolean).join(' ');
}

export function timeAgo(ts: string | undefined): string {
  if (!ts) return '—';
  const diff = Date.now() - new Date(ts).getTime();
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const mins = Math.floor(s / 60);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

export function getStatusColor(status: string): string {
  const s = status.toLowerCase();
  if (s === 'completed' || s === 'success') return '#4caf50';
  if (s === 'failed' || s === 'error' || s === 'critical') return '#f44336';
  if (s === 'gated') return '#9c27b0';
  if (s === 'running' || s === 'warning' || s === 'pending') return '#ff9800';
  if (s === 'canceled' || s === 'stopped') return '#607d8b';
  return '#9e9e9e';
}

/** Statuses for which a cancel request is meaningful. A gated run has no
 *  active lease to steal from, but the kernel still accepts the cancel —
 *  it abandons the obligation directly instead of waiting on review. */
export function isCancellable(status: string): boolean {
  return ['running', 'pending', 'retry', 'gated'].includes(status.toLowerCase());
}

export async function getDashboardMetrics(): Promise<DashboardMetrics> {
  const allRuns = await api.queryRuns(100);
  const cutoff = Date.now() - 86400000;
  const runs = allRuns.filter(r => new Date(r.started_at).getTime() >= cutoff);

  const total = runs.length;
  const successful = runs.filter(r => r.status.toLowerCase() === 'completed').length;
  const failed = runs.filter(r => ['failed','error','critical'].includes(r.status.toLowerCase())).length;
  const running = runs.filter(r => r.status.toLowerCase() === 'running').length;

  const failedRuns = runs.filter(r => ['failed','error','critical'].includes(r.status.toLowerCase()));
  const gatedRuns = runs.filter(r => r.status.toLowerCase() === 'gated');
  const longRunning = runs.filter(r => {
    if (!r.ended_at) return false;
    return new Date(r.ended_at).getTime() - new Date(r.started_at).getTime() > 7200000;
  });

  let totalMs = 0;
  runs.forEach(r => { if (r.ended_at) totalMs += new Date(r.ended_at).getTime() - new Date(r.started_at).getTime(); });

  return {
    healthStatus: failed > 0 ? 'red' : (longRunning.length > 0 || running > 0 || gatedRuns.length > 0) ? 'orange' : 'green',
    totalFlows: total, successfulFlows: successful, failedFlows: failed, runningFlows: running,
    successRate: total > 0 ? (successful / total) * 100 : 0,
    failureRate: total > 0 ? (failed / total) * 100 : 0,
    failedRuns, longRunningFlows: longRunning, gatedRuns,
    totalComputeTime: humanizeDuration(totalMs),
    averageExecutionTime: humanizeDuration(total > 0 ? totalMs / total : 0),
    lastUpdated: new Date(),
  };
}
