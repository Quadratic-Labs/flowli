import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Check, X } from 'lucide-react';
import { api } from '@/lib/api';

/** Resolve a obligation that is awaiting review: accept discharges it,
 *  reject reopens it for another attempt (or abandons it, if the attempt
 *  budget is spent). The kernel's gate policy — not this form — decides
 *  whether the given actor is eligible; a refusal surfaces as a 403. */
export function ReviewPanel({ obligationId }: { obligationId: string }) {
  const [actor, setActor] = useState('');
  const [reason, setReason] = useState('');
  const queryClient = useQueryClient();

  const mutation = useMutation({
    mutationFn: (decision: 'approved' | 'rejected') =>
      api.reviewObligation(obligationId, decision, actor.trim(), reason.trim() || undefined),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['obligations'] });
      queryClient.invalidateQueries({ queryKey: ['obligation', obligationId] });
      queryClient.invalidateQueries({ queryKey: ['obligation-events', obligationId] });
    },
  });

  const disabled = !actor.trim() || mutation.isPending;

  return (
    <div className="bg-purple-50 border border-purple-200 rounded p-4 mb-4">
      <h3 className="font-medium text-purple-900 mb-1">Awaiting review</h3>
      <p className="text-sm text-purple-700 mb-3">
        The attempt finished but is suspended for review — accept to discharge it,
        or reject to reopen it for another attempt (or abandon it, if none are left).
      </p>
      <div className="flex flex-wrap items-end gap-3">
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Actor</label>
          <input value={actor} onChange={e => setActor(e.target.value)}
            placeholder="e.g. lead"
            className="border border-gray-300 rounded px-2 py-1.5 text-sm w-40" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Reason (optional)</label>
          <input value={reason} onChange={e => setReason(e.target.value)}
            placeholder="optional"
            className="border border-gray-300 rounded px-2 py-1.5 text-sm w-56" />
        </div>
        <button onClick={() => mutation.mutate('approved')} disabled={disabled}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-green-600 text-white rounded hover:bg-green-700 disabled:opacity-50 text-sm">
          <Check size={14} /> Accept
        </button>
        <button onClick={() => mutation.mutate('rejected')} disabled={disabled}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-red-600 text-white rounded hover:bg-red-700 disabled:opacity-50 text-sm">
          <X size={14} /> Reject
        </button>
      </div>
      {mutation.isError && <p className="text-red-600 text-xs mt-2">{String(mutation.error)}</p>}
    </div>
  );
}
