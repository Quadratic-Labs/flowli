import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { RefreshCw } from 'lucide-react';
import { api, getStatusColor, timeAgo, humanizeDuration } from '@/lib/api';
import type { RunStateDTO } from '@/lib/api';

function statusAtIndex(statuses: string[], index: number): string | null {
  const offset = 5 - statuses.length;
  if (index < offset) return null;
  return [...statuses].reverse()[index - offset];
}

export default function FlowsList() {
  const flowsQ = useQuery({ queryKey: ['flows'], queryFn: api.getFlows });
  const runsQ = useQuery({ queryKey: ['runs', 100], queryFn: () => api.queryRuns(100) });

  const loading = flowsQ.isLoading || runsQ.isLoading;
  const error = flowsQ.error || runsQ.error;

  const flows = (() => {
    if (!flowsQ.data || !runsQ.data) return [];
    const byFlow = new Map<string, RunStateDTO[]>();
    runsQ.data.forEach(r => {
      if (!byFlow.has(r.flow_name)) byFlow.set(r.flow_name, []);
      const arr = byFlow.get(r.flow_name)!;
      if (arr.length < 5) arr.push(r);
    });
    return flowsQ.data.map(f => {
      const runs = byFlow.get(f.name) || [];
      const latest = runs[0];
      const dur = latest?.ended_at
        ? humanizeDuration(new Date(latest.ended_at).getTime() - new Date(latest.started_at).getTime())
        : null;
      return { name: f.name, doc: f.docstring ?? null, recent_statuses: runs.map(r => r.status),
        finished_ago: timeAgo(latest?.ended_at ?? undefined), duration: dur };
    });
  })();

  return (
    <div className="p-6">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-normal">Flowlet Workflows</h1>
        <button onClick={() => { flowsQ.refetch(); runsQ.refetch(); }}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
          <RefreshCw size={16} /> Refresh
        </button>
      </div>

      {loading && <div className="flex justify-center p-10"><div className="w-10 h-10 border-4 border-blue-600 border-t-transparent rounded-full animate-spin" /></div>}
      {error && <div className="text-red-600 bg-red-50 p-4 rounded">{String(error)}</div>}

      {!loading && !error && (
        flows.length === 0
          ? <div className="bg-blue-50 text-blue-700 p-4 rounded">No flows available.</div>
          : <table className="w-full text-sm shadow rounded overflow-hidden">
              <thead className="bg-gray-100 text-left">
                <tr>{['Flow Name','Recent Status','Finished','Duration','Description','Actions'].map(h =>
                  <th key={h} className="py-3 px-4">{h}</th>)}</tr>
              </thead>
              <tbody>{flows.map(f => (
                <tr key={f.name} className="border-t hover:bg-gray-50">
                  <td className="py-3 px-4 font-semibold">{f.name}</td>
                  <td className="py-3 px-4">
                    <div className="flex gap-1.5">
                      {[0,1,2,3,4].map(i => {
                        const s = statusAtIndex(f.recent_statuses, i);
                        return <span key={i} title={s ?? 'No run'}
                          style={{ backgroundColor: s ? getStatusColor(s) : undefined }}
                          className={`w-3 h-3 rounded-full inline-block transition-transform hover:scale-125 ${!s ? 'bg-gray-200 border border-gray-300 opacity-50' : ''}`} />;
                      })}
                    </div>
                  </td>
                  <td className="py-3 px-4 text-gray-500">{f.finished_ago}</td>
                  <td className="py-3 px-4 text-gray-500">{f.duration ?? '—'}</td>
                  <td className="py-3 px-4 text-gray-500">{f.doc ?? 'N/A'}</td>
                  <td className="py-3 px-4">
                    <Link to={`/flows/${f.name}/run`}
                      className="px-3 py-1.5 bg-blue-600 text-white rounded hover:bg-blue-700 text-sm">Run</Link>
                  </td>
                </tr>
              ))}</tbody>
            </table>
      )}
    </div>
  );
}
