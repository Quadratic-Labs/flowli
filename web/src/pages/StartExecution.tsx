import { useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useMutation, useQuery } from '@tanstack/react-query';
import { Play } from 'lucide-react';
import { api, ApiError, type JsonSchemaProperty } from '@/lib/api';

/** A form built from the workflow's JSON Schema, which the service derives
 *  from the signature. The service checks the arguments again and answers 422
 *  with the errors of its own validator, so this form guides rather than
 *  guards. */
export default function StartExecution() {
  const { name = '', version = '' } = useParams<{ name: string; version: string }>();
  const nav = useNavigate();
  const [values, setValues] = useState<Record<string, string>>({});
  const [dispatchKey, setDispatchKey] = useState('');

  const workflow = useQuery({
    queryKey: ['workflow', name, version],
    queryFn: () => api.workflow(name, version),
  });

  const start = useMutation({
    mutationFn: () => api.start({
      workflow: name,
      version,
      args: coerce(values, workflow.data?.schema?.properties ?? {}),
      dispatch_key: dispatchKey.trim() || undefined,
    }),
    onSuccess: result => nav(`/executions/${result.eid}`),
  });

  const schema = workflow.data?.schema;
  const properties = Object.entries(schema?.properties ?? {});
  const required = new Set(schema?.required ?? []);
  const fieldErrors = start.error instanceof ApiError ? start.error.problem.errors ?? [] : [];

  return (
    <div className="max-w-2xl space-y-4 p-6">
      <h1 className="text-2xl font-semibold text-slate-900">
        Start {name} <span className="text-lg font-normal text-slate-400">v{version}</span>
      </h1>
      {workflow.data?.description && (
        <p className="whitespace-pre-wrap text-sm text-slate-500">{workflow.data.description}</p>
      )}

      <form
        onSubmit={e => { e.preventDefault(); start.mutate(); }}
        className="space-y-4 rounded border border-slate-200 bg-white p-4"
      >
        {!schema && (
          <p className="rounded bg-amber-50 p-3 text-sm text-amber-800">
            This workflow has no schema, so its arguments are not checked. Give them as JSON.
          </p>
        )}

        {!schema ? (
          <textarea
            value={values.__raw ?? ''}
            onChange={e => setValues({ __raw: e.target.value })}
            rows={6}
            placeholder='{"invoice_id": "INV-7"}'
            className="w-full rounded border border-slate-300 p-2 font-mono text-sm"
          />
        ) : properties.length === 0 ? (
          <p className="text-sm text-slate-500">This workflow takes no arguments.</p>
        ) : (
          properties.map(([key, property]) => (
            <Field
              key={key}
              name={key}
              property={property}
              required={required.has(key)}
              value={values[key] ?? ''}
              error={fieldErrors.find(e => e.loc[0] === key)?.msg}
              onChange={v => setValues(prev => ({ ...prev, [key]: v }))}
            />
          ))
        )}

        <div className="border-t border-slate-100 pt-4">
          <label className="mb-1 block text-sm font-medium text-slate-700">
            Dispatch key <span className="font-normal text-slate-400">(optional)</span>
          </label>
          <input
            value={dispatchKey}
            onChange={e => setDispatchKey(e.target.value)}
            placeholder="e.g. invoice-7"
            className="w-full rounded border border-slate-300 px-3 py-2 text-sm"
          />
          <p className="mt-1 text-xs text-slate-400">
            Two starts with one key produce one execution: the second is told it was deduplicated.
          </p>
        </div>

        {start.error instanceof ApiError && !fieldErrors.length && (
          <p className="rounded bg-red-50 p-3 text-sm text-red-700">{start.error.message}</p>
        )}

        <button
          type="submit"
          disabled={start.isPending}
          className="flex items-center gap-2 rounded bg-indigo-600 px-4 py-2 text-white hover:bg-indigo-700 disabled:opacity-50"
        ><Play size={16} /> {start.isPending ? 'Starting…' : 'Start'}</button>
      </form>
    </div>
  );
}

/** The type of a property, seeing through the `anyOf` that an optional
 *  argument becomes in JSON Schema. */
function typeOf(property: JsonSchemaProperty): string {
  if (property.type) return property.type;
  const first = property.anyOf?.find(x => x.type && x.type !== 'null');
  return first?.type ?? 'string';
}

function Field({ name, property, required, value, error, onChange }: {
  name: string; property: JsonSchemaProperty; required: boolean;
  value: string; error?: string; onChange: (v: string) => void;
}) {
  const type = typeOf(property);
  const common = 'w-full rounded border px-3 py-2 text-sm ' +
    (error ? 'border-red-400 bg-red-50' : 'border-slate-300');

  return (
    <div>
      <label className="mb-1 block text-sm font-medium text-slate-700">
        {name}{required && <span className="text-red-500">*</span>}
        <span className="ml-1 font-normal text-slate-400">({type})</span>
      </label>
      {type === 'boolean' ? (
        <input
          type="checkbox"
          checked={value === 'true'}
          onChange={e => onChange(String(e.target.checked))}
          className="h-4 w-4"
        />
      ) : property.enum ? (
        <select value={value} onChange={e => onChange(e.target.value)} className={common}>
          <option value="">—</option>
          {property.enum.map(option => (
            <option key={String(option)} value={String(option)}>{String(option)}</option>
          ))}
        </select>
      ) : type === 'object' || type === 'array' ? (
        <textarea
          value={value} onChange={e => onChange(e.target.value)} rows={3}
          placeholder={type === 'array' ? '[]' : '{}'}
          className={`${common} font-mono`}
        />
      ) : (
        <input
          type={type === 'integer' || type === 'number' ? 'number' : 'text'}
          value={value}
          required={required}
          onChange={e => onChange(e.target.value)}
          placeholder={property.default !== undefined ? String(property.default) : ''}
          className={common}
        />
      )}
      {property.description && <p className="mt-1 text-xs text-slate-400">{property.description}</p>}
      {error && <p className="mt-1 text-xs text-red-600">{error}</p>}
    </div>
  );
}

/** Form fields are strings; the schema says what the service expects. An empty
 *  optional field is left out, so the workflow's own default applies. */
function coerce(
  values: Record<string, string>,
  properties: Record<string, JsonSchemaProperty>,
): Record<string, unknown> {
  if (values.__raw !== undefined) {
    try { return JSON.parse(values.__raw || '{}') as Record<string, unknown>; } catch { return {}; }
  }
  const out: Record<string, unknown> = {};
  for (const [key, raw] of Object.entries(values)) {
    if (raw === '') continue;
    const type = typeOf(properties[key] ?? {});
    if (type === 'boolean') out[key] = raw === 'true';
    else if (type === 'integer') out[key] = Number.parseInt(raw, 10);
    else if (type === 'number') out[key] = Number(raw);
    else if (type === 'object' || type === 'array') {
      try { out[key] = JSON.parse(raw); } catch { out[key] = raw; }
    } else out[key] = raw;
  }
  return out;
}
