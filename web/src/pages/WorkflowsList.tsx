import { Link } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { Play } from 'lucide-react';
import { api } from '@/lib/api';

/** The catalog: what this process can start. It shows the code of the process
 *  that answers, so two deployments can show two catalogs (spec 09 section 7). */
export default function WorkflowsList() {
  const { data, isLoading, error } = useQuery({ queryKey: ['workflows'], queryFn: api.workflows });

  return (
    <div className="space-y-4 p-6">
      <h1 className="text-2xl font-semibold text-slate-900">Workflows</h1>
      {isLoading && <p className="text-slate-400">loading…</p>}
      {error && <p className="rounded bg-red-50 p-3 text-red-700">{String(error)}</p>}

      <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
        {data?.items.map(wf => (
          <div key={`${wf.name}@${wf.version}`} className="rounded border border-slate-200 bg-white p-4">
            <div className="flex items-baseline gap-2">
              <h2 className="font-medium text-slate-900">{wf.name}</h2>
              <span className="text-sm text-slate-400">v{wf.version}</span>
            </div>
            <p className="mt-1 min-h-10 text-sm text-slate-500">{wf.summary ?? 'No summary.'}</p>
            <div className="mt-3 flex items-center gap-2">
              <Link
                to={`/workflows/${encodeURIComponent(wf.name)}/${encodeURIComponent(wf.version)}/start`}
                className="flex items-center gap-1.5 rounded bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-700"
              ><Play size={14} /> Start</Link>
              <span className="text-xs text-slate-400">queue {wf.queue}</span>
              {!wf.has_schema && (
                <span
                  className="ml-auto text-xs text-amber-600"
                  title="A parameter has no usable annotation, so the arguments are not checked."
                >unchecked</span>
              )}
            </div>
          </div>
        ))}
      </div>
      {!isLoading && !data?.items.length && <p className="text-slate-400">This process has no workflow registered.</p>}
    </div>
  );
}
