/** The client of the flowlet HTTP service. See flowlet/docs/specs/09-http-api.md.
 *
 *  Every noun here is the engine's: an execution runs a workflow, a frame is a
 *  node of its stack, a journal entry records what happened, evidence explains
 *  it. Nothing is an "obligation" any more — that model is gone.
 */

// --- the shapes the service returns ---------------------------------------

export type ExecutionStatus =
  | 'pending' | 'running' | 'suspended' | 'completed' | 'failed' | 'cancelled';

export interface Actor { kind: string | null; id: string | null; on_behalf_of?: string | null }

export interface ExecutionRow {
  eid: string;
  workflow: string;
  version: string;
  status: ExecutionStatus;
  queue: string;
  parent_eid: string | null;
  parent_fid: string | null;
  dispatch_key: string | null;
  created_at: string;
  updated_at: string;
  last_type: string | null;
  epoch: number | null;
  suspended_on: string[] | null;
  result: unknown;
  error: { type: string | null; message: string | null } | null;
  created_by: Actor;
  worker_id: string | null;
  host: string | null;
  archived_at: string | null;
}

export interface JournalEntry {
  seq: number;
  type: string;
  fid: string;
  payload: Record<string, unknown>;
  at: string;
  attempt: number;
  actor: Actor;
  site: { host: string | null; worker_id: string | null; epoch: number | null };
  code: {
    workflow: string; version: string;
    frame_kind: string; frame_name: string; code_ref: string | null;
  };
}

export type FrameStatus = 'running' | 'completed' | 'failed' | 'suspended' | 'cancelled';

export interface FrameNode {
  fid: string;
  kind: string;            // root | step | child | receive | sleep
  name: string;
  status: FrameStatus;
  attempts: number;
  started_at: string | null;
  ended_at: string | null;
  value: unknown;
  error: { type?: string; message?: string; retryable?: boolean; retry_at?: string } | null;
  suspended_on: string | null;
  deadline: string | null;
  evidence: boolean;
  children: FrameNode[];
}

export interface WorkflowSummary {
  name: string; version: string; summary: string | null; queue: string; has_schema: boolean;
}
export interface WorkflowDetail extends WorkflowSummary {
  description: string | null;
  schema: JsonSchema | null;
}

/** The subset of JSON Schema 2020-12 that a signature can produce. */
export interface JsonSchema {
  title?: string;
  type?: string;
  properties?: Record<string, JsonSchemaProperty>;
  required?: string[];
  $defs?: Record<string, JsonSchema>;
}
export interface JsonSchemaProperty {
  title?: string;
  type?: string;
  description?: string;
  default?: unknown;
  enum?: unknown[];
  anyOf?: JsonSchemaProperty[];
  items?: JsonSchemaProperty;
}

export interface ReviewRow {
  rid: string;
  eid: string | null;
  queue: string;
  status: 'pending' | 'decided' | 'expired';
  payload: unknown;
  deadline: string | null;
  requested_at: string;
  decided_at: string | null;
  verdict: string | null;
  decided_by: Actor | null;
}

export interface QueueDepth { queue: string; total: number; visible: number; claimed: number }

export interface TaskRow {
  task_id: string; queue: string; kind: string; eid: string; fid: string;
  reason: string; key: string; not_before: string | null;
  enqueued_at: string; enqueued_by: Actor;
}

export interface EvidenceItem {
  eid: string; fid: string; attempt: number; name: string; media_type: string; size: number;
}

export interface StartResponse { eid: string; deduplicated: boolean }
export interface DecideResponse {
  rid: string; verdict: string; decided_by: Actor; decided_at: string;
}
export interface Health {
  status: 'ok' | 'degraded'; projection_seq: number | null; checked_at: string; detail?: string;
}

interface Page<T> { items: T[]; next_cursor?: string | null }

/** An RFC 9457 problem body. `code` is the name of the domain error, so a
 *  caller branches on it without reading prose. */
export interface Problem {
  type?: string; title?: string; status?: number; code?: string; detail?: string;
  errors?: { loc: (string | number)[]; msg: string; type: string }[];
}

export class ApiError extends Error {
  status: number;
  code: string;
  problem: Problem;
  constructor(status: number, problem: Problem) {
    super(problem.detail ?? problem.title ?? `API error ${status}`);
    this.status = status;
    this.code = problem.code ?? 'error';
    this.problem = problem;
  }
}

// --- transport ------------------------------------------------------------

const API = '/api';

/** The access token. The service takes a bearer token and derives the actor
 *  from it: no request ever names its own actor (spec 09 section 5.2). */
let token: string | null = localStorage.getItem('flowlet.token');
export function setToken(value: string | null) {
  token = value;
  if (value) localStorage.setItem('flowlet.token', value);
  else localStorage.removeItem('flowlet.token');
}
export function getToken(): string | null { return token; }

/** ETag cache for GET endpoints: request with If-None-Match and, on a 304,
 *  hand back the exact same object as last time. Referential stability is the
 *  point — React Query (and any memo keyed on the data) then sees an unchanged
 *  value and nothing re-renders or redraws. The service gives every GET an
 *  ETag for exactly this (spec 09 section 4.3). */
const etagCache = new Map<string, { etag: string; data: unknown }>();

/** Failed polls get the same treatment: an identical failure re-throws the
 *  same Error instance, so `error` stays referentially stable across polls. */
const errorCache = new Map<string, { sig: string; error: ApiError }>();

/** The projection is eventually consistent. A write returns the sequence it
 *  reached; the next read of that resource passes it as `min_seq` and the
 *  service waits (spec 09 section 4.4). */
let lastControlSeq: number | null = null;
export function controlSeq(): number | null { return lastControlSeq; }

function withSeq(path: string): string {
  if (lastControlSeq === null) return path;
  return path + (path.includes('?') ? '&' : '?') + `min_seq=${lastControlSeq}`;
}

async function problemOf(res: Response): Promise<Problem> {
  try {
    return (await res.json()) as Problem;
  } catch {
    return { status: res.status, title: res.statusText };
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const method = (init?.method ?? 'GET').toUpperCase();
  const cached = method === 'GET' ? etagCache.get(path) : undefined;

  const headers: Record<string, string> = { ...(init?.headers as Record<string, string>) };
  if (init?.body && !headers['Content-Type']) headers['Content-Type'] = 'application/json';
  if (token) headers['Authorization'] = `Bearer ${token}`;
  if (cached) headers['If-None-Match'] = cached.etag;

  const res = await fetch(`${API}${path}`, { ...init, headers });

  if (res.status === 304 && cached) return cached.data as T;

  const seq = res.headers.get('X-Control-Seq');
  if (seq) lastControlSeq = Number(seq);

  if (!res.ok) {
    const problem = await problemOf(res);
    const sig = `${res.status}:${problem.code}:${problem.detail ?? ''}`;
    const previous = errorCache.get(path);
    if (previous && previous.sig === sig) throw previous.error;
    const error = new ApiError(res.status, problem);
    errorCache.set(path, { sig, error });
    throw error;
  }
  errorCache.delete(path);

  if (res.status === 204) return undefined as T;
  const data = (await res.json()) as T;
  const etag = res.headers.get('ETag');
  if (method === 'GET' && etag) etagCache.set(path, { etag, data });
  return data;
}

function query(params: Record<string, string | number | undefined | null>): string {
  const parts = Object.entries(params)
    .filter(([, v]) => v !== undefined && v !== null && v !== '')
    .map(([k, v]) => `${k}=${encodeURIComponent(String(v))}`);
  return parts.length ? `?${parts.join('&')}` : '';
}

// --- the endpoints --------------------------------------------------------

export const api = {
  // catalog
  workflows: () => request<Page<WorkflowSummary>>('/workflows'),
  workflow: (name: string, version: string) =>
    request<WorkflowDetail>(`/workflows/${encodeURIComponent(name)}/versions/${encodeURIComponent(version)}`),

  // executions
  start: (body: {
    workflow: string; version: string; args: Record<string, unknown>;
    queue?: string; dispatch_key?: string;
  }) => request<StartResponse>('/executions', { method: 'POST', body: JSON.stringify(body) }),

  executions: (params: {
    status?: string; workflow?: string; parent_eid?: string; limit?: number; cursor?: string;
  } = {}) => request<Page<ExecutionRow>>(withSeq(`/executions${query(params)}`)),

  execution: (eid: string) => request<ExecutionRow>(withSeq(`/executions/${eid}`)),
  journal: (eid: string, after = 0) =>
    request<{ items: JournalEntry[]; tail: number }>(`/executions/${eid}/journal${query({ after })}`),
  frames: (eid: string) =>
    request<{ root: FrameNode | null; tail: number }>(`/executions/${eid}/frames`),
  children: (eid: string) => request<Page<ExecutionRow>>(`/executions/${eid}/children`),

  signal: (eid: string, channel: string, payload: unknown) =>
    request<{ seq: number }>(`/executions/${eid}/signal`, {
      method: 'POST', body: JSON.stringify({ channel, payload }),
    }),
  cancel: (eid: string) =>
    request<{ eid: string; cancel_requested: boolean; tasks_cancelled: string[] }>(
      `/executions/${eid}/cancel`, { method: 'POST' }),
  migrate: (eid: string, version: string) =>
    request<{ eid: string; version: string }>(`/executions/${eid}/migrate`, {
      method: 'POST', body: JSON.stringify({ version }),
    }),

  // reviews
  reviews: (queue?: string) => request<Page<ReviewRow>>(withSeq(`/reviews${query({ queue })}`)),
  decide: (rid: string, verdict: string, data?: unknown) =>
    request<DecideResponse>(`/reviews/${encodeURIComponent(rid)}/decide`, {
      method: 'POST', body: JSON.stringify({ verdict, data: data ?? null }),
    }),

  // queues and health
  queues: () => request<{ items: QueueDepth[] }>('/queues'),
  tasks: (queue: string, limit = 50) =>
    request<Page<TaskRow>>(`/queues/${encodeURIComponent(queue)}/tasks${query({ limit })}`),
  stats: () => request<{ by_status: Record<string, number> }>(withSeq('/stats')),
  health: () => request<Health>('/health'),

  // evidence
  evidence: (eid: string) => request<{ items: EvidenceItem[] }>(`/evidence/${eid}`),
  evidenceUrl: (eid: string, fid: string, attempt: number, name: string) =>
    `${API}/evidence/${eid}/attempts/${attempt}/${encodeURIComponent(name)}${query({ fid })}`,
  /** The fid is a query parameter: a frame id holds `/`, and a percent-encoded
   *  slash does not survive routing (spec 09 section 10). */
  attemptLog: async (eid: string, fid: string, attempt: number): Promise<string> => {
    const res = await fetch(api.evidenceUrl(eid, fid, attempt, 'log'), {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
    if (!res.ok) throw new ApiError(res.status, await problemOf(res));
    return res.text();
  },
};
