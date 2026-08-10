import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { RefreshCw } from 'lucide-react';
import { api, isCancellable } from '@/lib/api';
import { StatusBadge } from '@/components/StatusBadge';
import { CancelRunButton } from '@/components/CancelRunButton';

export default function RunsList() {
  const { data: runs = [], isLoading, error, refetch } = useQuery({
    queryKey: ['runs', 50], queryFn: () => api.queryRuns(50),
  });

  return (
    <div className="p-6">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-normal">Flow Runs</h1>
        <button onClick={() => refetch()} className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
          <RefreshCw size={16} /> Refresh
        </button>
      </div>

      {isLoading && <div className="flex justify-center p-10"><div className="w-10 h-10 border-4 border-blue-600 border-t-transparent rounded-full animate-spin" /></div>}
      {error && <div className="text-red-600 bg-red-50 p-4 rounded">{String(error)}</div>}

      {!isLoading && !error && (
        runs.length === 0
          ? <div className="bg-blue-50 text-blue-700 p-4 rounded">No runs found.</div>
          : <table className="w-full text-sm shadow rounded overflow-hidden">
              <thead className="bg-gray-100 text-left">
                <tr>{['Flow Name','Status','Started At','Finished At','Actions'].map(h =>
                  <th key={h} className="py-3 px-4">{h}</th>)}</tr>
              </thead>
              <tbody>{runs.map(r => (
                <tr key={r.run_id} className="border-t hover:bg-gray-50">
                  <td className="py-3 px-4 font-semibold">{r.flow_name}</td>
                  <td className="py-3 px-4"><StatusBadge status={r.status} /></td>
                  <td className="py-3 px-4 text-gray-500">{new Date(r.started_at).toLocaleString()}</td>
                  <td className="py-3 px-4 text-gray-500">{r.ended_at ? new Date(r.ended_at).toLocaleString() : 'Running...'}</td>
                  <td className="py-3 px-4">
                    <div className="flex items-center gap-2">
                      <Link to={`/runs/${r.run_id}`}
                        className="px-3 py-1.5 bg-purple-600 text-white rounded hover:bg-purple-700 text-sm">View Details</Link>
                      {isCancellable(r.status) && <CancelRunButton runId={r.run_id} small />}
                    </div>
                  </td>
                </tr>
              ))}</tbody>
            </table>
      )}
    </div>
  );
}
