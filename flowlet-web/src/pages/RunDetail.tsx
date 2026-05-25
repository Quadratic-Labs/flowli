import { useState } from 'react';
import { useParams, useSearchParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { ChevronDown, ChevronRight } from 'lucide-react';
import { api } from '@/lib/api';
import { StatusBadge } from '@/components/StatusBadge';
import { RunFlamegraph } from '@/components/RunFlamegraph';

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

  const logs = run?.logs ?? [];
  const children = run?.children ?? [];

  return (
    <div className="p-6">
      {isLoading && <div className="flex justify-center p-10"><div className="w-10 h-10 border-4 border-blue-600 border-t-transparent rounded-full animate-spin" /></div>}
      {error && <div className="text-red-600 bg-red-50 p-4 rounded">{String(error)}</div>}

      {run && !isLoading && (
        <>
          <div className="bg-white rounded shadow p-6 mb-4">
            <h2 className="text-lg font-medium mb-4">Run Details</h2>
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <p><strong>Name:</strong> {run.span_name}</p>
                <p><strong>Run ID:</strong> {run.span_id}</p>
                <p><strong>Status:</strong> <StatusBadge status={run.status} /></p>
              </div>
              <div>
                {parent && <p><strong>Parent:</strong> {parent.span_name} ({parent.span_id})</p>}
                {run.duration && <p><strong>Duration:</strong> {run.duration}</p>}
              </div>
            </div>
          </div>

          <Section title="Execution Flamegraph" icon={<span>⏱</span>} defaultOpen>
            <RunFlamegraph runData={run} />
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
