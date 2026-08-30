import { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { Play } from 'lucide-react';
import { api } from '@/lib/api';

export default function FlowRun() {
  const { name = '' } = useParams<{ name: string }>();
  const nav = useNavigate();
  const [values, setValues] = useState<Record<string, unknown>>({});
  const [dispatchKey, setDispatchKey] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);

  const { data: flows, isLoading, error } = useQuery({ queryKey: ['flows'], queryFn: api.getFlows });
  const schema = flows?.find(f => f.name === name);

  function isBool(t: string) { return ['bool','boolean'].includes(t.toLowerCase()); }
  function inputType(t: string) { return ['int','float','number'].some(x => t.toLowerCase().includes(x)) ? 'number' : 'text'; }

  function getDefault(p: { type: string; default?: unknown }): unknown {
    if (p.default !== undefined && p.default !== null) return p.default;
    if (isBool(p.type)) return false;
    return '';
  }

  function getValue(param: { name: string; type: string; default?: unknown }) {
    return values[param.name] ?? getDefault(param);
  }

  async function handleSubmit(e: React.SyntheticEvent<HTMLFormElement>) {
    e.preventDefault();
    setSubmitting(true);
    try {
      const kwargs: Record<string, unknown> = {};
      (schema?.parameters ?? []).forEach(p => {
        const v = getValue(p);
        if (v !== null && v !== '') kwargs[p.name] = v;
      });
      const resp = await api.submitFlow(name, kwargs, dispatchKey.trim() || undefined);
      setToast({
        msg: resp.deduplicated
          ? `Dispatch key already submitted — resolved to existing run ${resp.obligation_id.slice(0, 8)}…`
          : `Flow "${name}" submitted as run ${resp.obligation_id.slice(0, 8)}…`,
        ok: true,
      });
      setTimeout(() => nav(`/runs/${resp.obligation_id}?flow=${encodeURIComponent(name)}`), 1500);
    } catch (err) {
      setToast({ msg: `Failed: ${err}`, ok: false });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="p-6 max-w-2xl">
      <h1 className="text-3xl font-normal mb-6">Run Flow: {name}</h1>
      {isLoading && <div className="flex justify-center p-10"><div className="w-10 h-10 border-4 border-blue-600 border-t-transparent rounded-full animate-spin" /></div>}
      {error && <div className="text-red-600 bg-red-50 p-4 rounded">{String(error)}</div>}
      {!isLoading && !error && !schema && <div className="text-red-600 bg-red-50 p-4 rounded">Flow "{name}" not found.</div>}

      {toast && (
        <div className={`mb-4 p-3 rounded text-sm ${toast.ok ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'}`}>
          {toast.msg}
        </div>
      )}

      {schema && (
        <div className="bg-white rounded shadow p-6">
          <h2 className="text-xl font-medium mb-2">{schema.name}</h2>
          {schema.docstring && <p className="text-gray-500 mb-4">{schema.docstring}</p>}

          <form onSubmit={handleSubmit} className="space-y-4">
            {(!schema.parameters || schema.parameters.length === 0) && (
              <div className="bg-blue-50 text-blue-700 p-3 rounded">This flow has no parameters. Click "Run Flow" to execute it.</div>
            )}

            {(schema.parameters ?? []).map(param => (
              <div key={param.name}>
                {isBool(param.type) ? (
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input type="checkbox" checked={Boolean(getValue(param))}
                      onChange={e => setValues(v => ({ ...v, [param.name]: e.target.checked }))}
                      className="w-4 h-4" />
                    <span>{param.name}{param.required && <span className="text-red-500">*</span>}
                      <span className="text-gray-400 text-sm ml-2">({param.type})</span></span>
                  </label>
                ) : (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      {param.name}{param.required && <span className="text-red-500">*</span>}
                      <span className="text-gray-400 ml-1">({param.type})</span>
                    </label>
                    <input type={inputType(param.type)} value={String(getValue(param) ?? '')}
                      onChange={e => setValues(v => ({ ...v, [param.name]: e.target.value }))}
                      required={param.required}
                      placeholder={`Enter ${param.name}`}
                      className="w-full border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500" />
                  </div>
                )}
              </div>
            ))}

            <div className="border-t pt-4">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Dispatch key
                <span className="text-gray-400 ml-1 font-normal">(optional — idempotency)</span>
              </label>
              <input type="text" value={dispatchKey}
                onChange={e => setDispatchKey(e.target.value)}
                placeholder="e.g. webhook-delivery-42 or campaign-sync:2026-08-04"
                className="w-full border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500" />
              <p className="text-xs text-gray-400 mt-1">
                Submissions with the same key collapse onto one run — safe against
                double-clicks, webhook retries, and overlapping schedules.
              </p>
            </div>

            <div className="pt-2">
              <button type="submit" disabled={submitting}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50">
                {submitting ? <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" /> : <Play size={16} />}
                {submitting ? 'Running...' : 'Run Flow'}
              </button>
            </div>
          </form>
        </div>
      )}
    </div>
  );
}
