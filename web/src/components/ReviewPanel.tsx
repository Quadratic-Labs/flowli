import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Check, X } from 'lucide-react';
import { api, ApiError, type ReviewRow } from '@/lib/api';
import { json } from '@/lib/format';

/** Decide one review. The verdict is delivered to the workflow, which decides
 *  what it means: the service records who decided, and the engine checks
 *  nothing (spec 06 section 2).
 *
 *  There is no actor field. The service takes the actor from the access token,
 *  because a review signed by whoever the client claimed to be is worth
 *  nothing (spec 09 section 5.2). */
export function ReviewPanel({ review, onDecided }: {
  review: ReviewRow;
  onDecided?: () => void;
}) {
  const [note, setNote] = useState('');
  const client = useQueryClient();

  const decide = useMutation({
    mutationFn: (verdict: string) =>
      api.decide(review.rid, verdict, note.trim() ? { note: note.trim() } : undefined),
    onSuccess: () => {
      client.invalidateQueries({ queryKey: ['reviews'] });
      if (review.eid) client.invalidateQueries({ queryKey: ['execution', review.eid] });
      onDecided?.();
    },
  });

  const decided = review.status !== 'pending';

  return (
    <div className="rounded border border-purple-200 bg-purple-50 p-4">
      <div className="mb-2 flex items-center gap-2">
        <h3 className="font-medium text-purple-900">Review · {review.queue}</h3>
        {review.deadline && (
          <span className="text-xs text-purple-700">due {review.deadline.slice(0, 19)}</span>
        )}
      </div>

      <pre className="mb-3 max-h-48 overflow-auto rounded bg-white/70 p-2 text-xs text-slate-700">
        {json(review.payload)}
      </pre>

      {decided ? (
        <p className="text-sm text-purple-800">
          {review.verdict} by {review.decided_by?.id ?? 'unknown'}
          {review.decided_at ? ` · ${review.decided_at.slice(0, 19)}` : ''}
        </p>
      ) : (
        <>
          <input
            value={note}
            onChange={e => setNote(e.target.value)}
            placeholder="note (optional, delivered with the decision)"
            className="mb-3 w-full rounded border border-purple-200 px-2 py-1.5 text-sm"
          />
          <div className="flex items-center gap-2">
            <button
              onClick={() => decide.mutate('approve')}
              disabled={decide.isPending}
              className="flex items-center gap-1.5 rounded bg-green-600 px-3 py-1.5 text-sm text-white hover:bg-green-700 disabled:opacity-50"
            ><Check size={14} /> Approve</button>
            <button
              onClick={() => decide.mutate('reject')}
              disabled={decide.isPending}
              className="flex items-center gap-1.5 rounded bg-red-600 px-3 py-1.5 text-sm text-white hover:bg-red-700 disabled:opacity-50"
            ><X size={14} /> Reject</button>
            {decide.error instanceof ApiError && (
              <span className="text-sm text-red-700">
                {decide.error.code === 'forbidden'
                  ? `You may not decide reviews on ${review.queue}.`
                  : decide.error.message}
              </span>
            )}
          </div>
        </>
      )}
    </div>
  );
}
