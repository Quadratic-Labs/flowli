import { useQuery } from '@tanstack/react-query';
import { FileText, Paperclip } from 'lucide-react';
import { api, type EvidenceItem } from '@/lib/api';

/** Evidence explains the journal; it is never authority (spec 03 section 12).
 *  The frame tree says which frames wrote some, and this reads it only when a
 *  person opens the frame. */
export function EvidencePanel({ eid, fid, items }: {
  eid: string; fid: string; items: EvidenceItem[];
}) {
  const attempts = [...new Set(items.map(i => i.attempt))].sort((a, b) => a - b);
  if (!items.length) {
    return <p className="text-sm text-slate-500">This frame wrote no evidence.</p>;
  }
  return (
    <div className="space-y-4">
      {attempts.map(attempt => (
        <AttemptEvidence
          key={attempt}
          eid={eid}
          fid={fid}
          attempt={attempt}
          items={items.filter(i => i.attempt === attempt)}
        />
      ))}
    </div>
  );
}

function AttemptEvidence({ eid, fid, attempt, items }: {
  eid: string; fid: string; attempt: number; items: EvidenceItem[];
}) {
  const hasLog = items.some(i => i.name === 'log');
  const attachments = items.filter(i => i.name !== 'log');

  const { data: log, isLoading, error } = useQuery({
    queryKey: ['evidence-log', eid, fid, attempt],
    queryFn: () => api.attemptLog(eid, fid, attempt),
    enabled: hasLog,
    staleTime: 30_000,
  });

  return (
    <div className="rounded border border-slate-200">
      <div className="flex items-center gap-2 border-b border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-medium text-slate-600">
        Attempt {attempt}
        <span className="text-slate-400">
          {/* The key of evidence is the attempt: a retry writes a second log. */}
          {items.reduce((n, i) => n + i.size, 0)} bytes
        </span>
      </div>
      {hasLog && (
        <div className="max-h-72 overflow-auto bg-slate-900 p-3 font-mono text-xs text-slate-100">
          {isLoading && <span className="text-slate-400">loading…</span>}
          {error && <span className="text-red-300">{String(error)}</span>}
          {log?.split('\n').filter(Boolean).map((line, i) => <LogLine key={i} raw={line} />)}
        </div>
      )}
      {attachments.length > 0 && (
        <ul className="divide-y divide-slate-100 text-sm">
          {attachments.map(item => (
            <li key={item.name} className="flex items-center gap-2 px-3 py-2">
              <Paperclip size={14} className="text-slate-400" />
              <a
                href={api.evidenceUrl(eid, fid, attempt, item.name)}
                target="_blank" rel="noreferrer"
                className="text-indigo-600 hover:underline"
              >{item.name}</a>
              <span className="text-xs text-slate-400">{item.media_type} · {item.size} bytes</span>
            </li>
          ))}
        </ul>
      )}
      {!hasLog && !attachments.length && (
        <p className="px-3 py-2 text-sm text-slate-500">
          <FileText size={14} className="mr-1 inline" /> nothing recorded
        </p>
      )}
    </div>
  );
}

const LEVEL_COLOR: Record<string, string> = {
  error: 'text-red-300', warning: 'text-amber-300', info: 'text-slate-100', debug: 'text-slate-400',
};

/** One JSON Lines record. The engine binds `eid`, `fid` and `attempt` to every
 *  event already, so those are dropped here: the panel is inside that frame. */
function LogLine({ raw }: { raw: string }) {
  let record: Record<string, unknown>;
  try {
    record = JSON.parse(raw) as Record<string, unknown>;
  } catch {
    return <div className="whitespace-pre-wrap">{raw}</div>;
  }
  const { event, level, timestamp, eid, fid, attempt, epoch, worker_id, task_id, ...rest } = record;
  void eid; void fid; void attempt; void epoch; void worker_id; void task_id;
  const colour = LEVEL_COLOR[String(level ?? 'info')] ?? 'text-slate-100';
  return (
    <div className="whitespace-pre-wrap">
      <span className="text-slate-500">{String(timestamp ?? '').slice(11, 23)} </span>
      <span className={colour}>{String(event ?? '')}</span>
      {Object.entries(rest).map(([k, v]) => (
        <span key={k} className="text-slate-400"> {k}=<span className="text-sky-300">{
          typeof v === 'string' ? v : JSON.stringify(v)
        }</span></span>
      ))}
    </div>
  );
}
