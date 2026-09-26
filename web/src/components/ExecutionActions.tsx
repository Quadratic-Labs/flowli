import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Ban, Send, Wand2 } from 'lucide-react';
import { api, ApiError, type ExecutionRow } from '@/lib/api';
import { isTerminal } from '@/lib/format';

/** Cancel, signal and migrate: the three operator writes of spec 09 section 8.4.
 *  Each is a capability the token may not carry, so a refusal is shown as what
 *  it is rather than as a failure of the app. */
export function ExecutionActions({ execution }: { execution: ExecutionRow }) {
  const client = useQueryClient();
  const [channel, setChannel] = useState('');
  const [payload, setPayload] = useState('');
  const [version, setVersion] = useState(execution.version);
  const terminal = isTerminal(execution.status);

  const refresh = () => {
    client.invalidateQueries({ queryKey: ['execution', execution.eid] });
    client.invalidateQueries({ queryKey: ['journal', execution.eid] });
    client.invalidateQueries({ queryKey: ['frames', execution.eid] });
  };

  const cancel = useMutation({ mutationFn: () => api.cancel(execution.eid), onSuccess: refresh });
  const signal = useMutation({
    mutationFn: () => {
      let body: unknown = payload;
      try { body = payload ? JSON.parse(payload) : null; } catch { /* a string is a payload too */ }
      return api.signal(execution.eid, channel.trim(), body);
    },
    onSuccess: () => { setPayload(''); refresh(); },
  });
  const migrate = useMutation({
    mutationFn: () => api.migrate(execution.eid, version.trim()),
    onSuccess: refresh,
  });

  const failure = [cancel.error, signal.error, migrate.error].find(Boolean);

  return (
    <div className="space-y-3 rounded border border-slate-200 bg-white p-4">
      <h3 className="text-sm font-medium text-slate-700">Operator</h3>

      <div className="flex flex-wrap items-end gap-2">
        <div className="grow">
          <label className="mb-1 block text-xs text-slate-500">Channel</label>
          <input
            value={channel} onChange={e => setChannel(e.target.value)}
            placeholder="e.g. payments" disabled={terminal}
            className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm disabled:bg-slate-50"
          />
        </div>
        <div className="grow-[2]">
          <label className="mb-1 block text-xs text-slate-500">Payload (JSON, or plain text)</label>
          <input
            value={payload} onChange={e => setPayload(e.target.value)}
            placeholder='{"amount": 100}' disabled={terminal}
            className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm disabled:bg-slate-50"
          />
        </div>
        <button
          onClick={() => signal.mutate()}
          disabled={terminal || !channel.trim() || signal.isPending}
          className="flex items-center gap-1.5 rounded bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-700 disabled:opacity-40"
        ><Send size={14} /> Signal</button>
      </div>

      <div className="flex flex-wrap items-end gap-2 border-t border-slate-100 pt-3">
        <div>
          <label className="mb-1 block text-xs text-slate-500">Version</label>
          <input
            value={version} onChange={e => setVersion(e.target.value)}
            disabled={terminal}
            className="w-24 rounded border border-slate-300 px-2 py-1.5 text-sm disabled:bg-slate-50"
          />
        </div>
        <button
          onClick={() => migrate.mutate()}
          disabled={terminal || version === execution.version || migrate.isPending}
          className="flex items-center gap-1.5 rounded border border-slate-300 px-3 py-1.5 text-sm hover:bg-slate-50 disabled:opacity-40"
          title="Replay this execution against another version of the workflow"
        ><Wand2 size={14} /> Migrate</button>
        <div className="grow" />
        <button
          onClick={() => cancel.mutate()}
          disabled={terminal || cancel.isPending}
          className="flex items-center gap-1.5 rounded bg-red-600 px-3 py-1.5 text-sm text-white hover:bg-red-700 disabled:opacity-40"
        ><Ban size={14} /> Cancel</button>
      </div>

      {cancel.data && cancel.data.tasks_cancelled.length > 0 && (
        <p className="text-xs text-slate-500">
          Asked {cancel.data.tasks_cancelled.length} delegate consumer(s) to stop as well.
        </p>
      )}
      {failure instanceof ApiError && (
        <p className="text-sm text-red-700">
          {failure.code === 'forbidden'
            ? 'Your token does not carry that capability.'
            : failure.message}
        </p>
      )}
      {terminal && (
        <p className="text-xs text-slate-400">
          A terminal execution takes no signal, no cancel and no migration.
        </p>
      )}
    </div>
  );
}
